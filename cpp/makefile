.DEFAULT_GOAL = all

%: 
	mkdir -p build && cd build && cmake .. && cd ..
	$(MAKE) -C build $@
