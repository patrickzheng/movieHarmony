tmux new -s <session name>
# then start your process.  Now you can log out, close window, get timed out and process will keep going.  
# To list available sessions, enter following on remote host:
tmux ls
# To attach to an existing session:
tmux a -t <target session>
