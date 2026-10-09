# Build and test the MapReduce lab.
#
#   make mr                      run all tests
#   make RUN="-run Wc" mr        run a single test (Wc, Indexer, MapParallel,
#                                ReduceParallel, JobCount, EarlyExit, CrashWorker)

VERB=-v
RACE=-race
APPS=wc indexer mtiming rtiming jobcount early_exit crash nocrash

.PHONY: mr build clean

mr: build
	cd mr && go test $(VERB) $(RACE) $(RUN)

build:
	go build $(RACE) -o main/mrsequential main/mrsequential.go
	go build $(RACE) -o main/mrcoordinator main/mrcoordinator.go
	go build $(RACE) -o main/mrworker main/mrworker.go
	for a in $(APPS); do (cd mrapps && go build $(RACE) -buildmode=plugin $$a.go) || exit 1; done

clean:
	rm -f main/mrsequential main/mrcoordinator main/mrworker mrapps/*.so
	rm -rf mr/mr-tmp-*
	rm -f main/mr-out-* main/mr-*-*
