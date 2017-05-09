TARGETS=dm-attach-file dm-dump-xls dm-export-repo dm-import-csv dm-import-datamapping dm-run-profiler dm-run-server dm-verify-results

all: ja.mo

japot:
	./mkpo.sh ja

ja.mo: po/ja.po
	mkdir -p hecatoncheir/locale/ja_JP/LC_MESSAGES
	msgfmt -o hecatoncheir/locale/ja_JP/LC_MESSAGES/hecatoncheir.mo po/ja.po

pep8:
	for f in $(TARGETS); do \
	  cp $$f $$f.py; \
	done;
	py.test --pep8 dm-*.py
	rm dm-*.py
	make -C hecatoncheir pep8

clean:
	rm -rf *~
	rm -rf hecatoncheir.egg-info
	rm -f dm-*.py *.pot
	find . -name .cache -type d | xargs rm -rf
	find . -name __pycache__ -type d | xargs rm -rf
	find . -name build -type d | xargs rm -rf
	find . -name dist -type d | xargs rm -rf
	make -C hecatoncheir clean
	make -C tests clean
