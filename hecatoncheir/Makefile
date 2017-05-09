# Makefile
TARGET=DbProfiler DbProfilerExp DbProfilerMerge DbProfilerVerify DbProfilerViewer DataMappingImp dm-attach-file

all: $(TARGET)

%: %.py
	cp $< $@
	chmod 755 $@

clean:
	rm -f *.pyc $(TARGET)
	find . -name '*.pyc' -exec rm {} \;
	find . -name '*~' -exec rm {} \;

pep8:
	find . -name '*.py' | xargs py.test --pep8
