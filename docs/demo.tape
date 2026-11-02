# Records deploy/demo.sh as docs/images/demo.gif with VHS
# (https://github.com/charmbracelet/vhs; `brew install vhs`).
#
# With the stack running (docker compose up -d), from the repo root:
#   vhs docs/demo.tape

Output docs/images/demo.gif
Require curl
Require jq
Require docker

Set Shell bash
Set FontSize 15
Set Width 1300
Set Height 760
Set Padding 16
Set TypingSpeed 40ms

Type "deploy/demo.sh"
Enter
Wait+Screen /Grafana dashboard/
Sleep 3s
