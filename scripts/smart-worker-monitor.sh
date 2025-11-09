#!/bin/bash

# Smart Worker Monitor - Monitors worker and shuts it down when idle
# Usage: smart-worker-monitor.sh [idle-timeout-seconds]
# This script monitors the worker process and stops it via supervisor when idle

IDLE_TIMEOUT=${1:-300}  # Default 5 minutes (300 seconds)
CHECK_INTERVAL=60       # Check every minute
SCRIPT_NAME="smart-worker-monitor"
LOG_PREFIX="[MONITOR]"

# Ensure only one monitor instance runs
PIDFILE="/tmp/${SCRIPT_NAME}.pid"

# Function to clean up on exit
cleanup() {
    echo "$LOG_PREFIX Monitor shutting down..."
    rm -f "$PIDFILE"
    exit 0
}

# Set up signal handlers
trap cleanup SIGTERM SIGINT EXIT

# Check if another monitor is already running
if [ -f "$PIDFILE" ]; then
    EXISTING_PID=$(cat "$PIDFILE")
    if kill -0 "$EXISTING_PID" 2>/dev/null; then
        echo "$LOG_PREFIX Monitor already running with PID $EXISTING_PID"
        exit 1
    else
        echo "$LOG_PREFIX Removing stale PID file"
        rm -f "$PIDFILE"
    fi
fi

# Write our PID
echo $$ > "$PIDFILE"

echo "$LOG_PREFIX Starting worker monitor with ${IDLE_TIMEOUT}s idle timeout..."

# Function to check if worker is running via supervisor
is_worker_running() {
    supervisorctl status worker 2>/dev/null | grep -q "RUNNING"
    return $?
}

# Function to check pending commands via API
check_pending_commands() {
    # Use API to check for pending jobs
    # GET /api/commands/jobs?limit=50 returns list of recent jobs
    
    local api_response=$(curl -s "http://localhost:5055/api/commands/jobs?limit=50" 2>/dev/null)
    
    if [ $? -ne 0 ]; then
        echo "$LOG_PREFIX Warning: Could not reach API to check pending jobs"
        return 1  # Assume no pending commands on API error
    fi
    
    # Count jobs that are not completed or failed
    # Look for "status": "new", "running", "submitted" etc (anything not "completed" or "failed")
    local pending_count=$(echo "$api_response" | grep -o '"status":\s*"[^"]*"' | grep -v '"completed"' | grep -v '"failed"' | wc -l)
    
    if [ "$pending_count" -gt 0 ]; then
        echo "$LOG_PREFIX Found $pending_count pending jobs"
        return 0  # Has pending commands
    else
        echo "$LOG_PREFIX No pending jobs found"
        return 1  # No pending commands
    fi
}

# Function to stop worker via supervisor
stop_worker() {
    echo "$LOG_PREFIX Stopping worker via supervisor..."
    supervisorctl stop worker
    return $?
}

# Main monitoring loop
last_activity=$(date +%s)
consecutive_idle_checks=0

echo "$LOG_PREFIX Monitor started, checking every ${CHECK_INTERVAL}s"

while true; do
    # First check if worker is still running
    if ! is_worker_running; then
        echo "$LOG_PREFIX Worker is not running, monitor exiting"
        break
    fi
    
    # Check for pending commands (placeholder implementation)
    if check_pending_commands; then
        # Reset idle timer when we find pending work
        last_activity=$(date +%s)
        consecutive_idle_checks=0
        echo "$LOG_PREFIX Activity detected, resetting idle timer"
    else
        consecutive_idle_checks=$((consecutive_idle_checks + 1))
        current_time=$(date +%s)
        idle_time=$((current_time - last_activity))
        
        echo "$LOG_PREFIX Idle check $consecutive_idle_checks, total idle time: ${idle_time}s"
        
        # Check if we've been idle too long
        if [ $idle_time -ge $IDLE_TIMEOUT ]; then
            echo "$LOG_PREFIX Worker has been idle for ${idle_time}s (threshold: ${IDLE_TIMEOUT}s)"
            echo "$LOG_PREFIX Shutting down worker..."
            
            if stop_worker; then
                echo "$LOG_PREFIX Worker stopped successfully, monitor exiting"
                break
            else
                echo "$LOG_PREFIX Failed to stop worker, continuing monitoring..."
            fi
        fi
    fi
    
    # Wait before next check
    sleep $CHECK_INTERVAL
done

echo "$LOG_PREFIX Monitor finished"
