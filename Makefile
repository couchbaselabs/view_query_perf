EBIN_DIR = ebin
SRC_DIR = src
ERLC = erlc
SCRIPT = run

all: clean ebin

ebin: $(SRC_DIR)/*.*
	mkdir -p $(EBIN_DIR)
	$(ERLC) -DTEST -o $(EBIN_DIR) -I $(SRC_DIR) $(SRC_DIR)/*.erl
	cd ebin && zip -9 ../ebin.zip *.beam && cd ..
	echo '#!/usr/bin/env escript' > $(SCRIPT)
	echo "%%! -smp enable -escript main view_query_perf" >> $(SCRIPT)
	cat ebin.zip >> $(SCRIPT)
	chmod +x $(SCRIPT)
	rm -f ebin.zip

clean:
	rm -fr $(EBIN_DIR)
	rm -f $(SCRIPT)
