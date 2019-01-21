## Task
Write a user-space script utility which functionally tests the three data transfer protocols, XrootD, Rados, and GridFTP, providing detailed information about the performance that may be used to diagnose simple problems at a glance (e.g. connectivity.) The aim is that an Echo admin on-call can quickly run the script as part of initial standard procedure.

## Issues
XrootD requires a temporary grid proxy certificate in order to function, so this must be reliably created and handled in order to test XrootD functioning.

The current script does not have any GridFTP commands, and only has a partially written XrootD command.

## Suggestions
- Check ral-ceph-tools for the latest version of the current scripts
- Find out how the XrootD grid proxy works, perhaps by trying it out on an lcguiXY machine and performing a manual functional test. 
- Ensure the script is robust against bad user input.

