
# Creating & updating a po file

Use `mkpo.sh` command to create a (updated) pot file.

Then, edit `LANG.pot` file.

Rename it to `LANG.po` and place it in the po directory.

Add new `LANG.mo` target in the Makefile to copy it to the locale
directory as you need.
