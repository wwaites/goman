include $(GOROOT)/src/Make.inc

TARG=github.com/wwaites/goman
GOFILES=\
	gearman.go\
	client.go\
	call.go\
	worker.go\
	util.go\

include $(GOROOT)/src/Make.pkg
