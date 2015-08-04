REBAR := ./rebar

.PHONY: all doc clean test eunit dialyze test

all: compile doc

test: eunit xref dialyze

compile:
	$(REBAR) compile

doc:
	$(REBAR) doc

eunit: compile
	$(REBAR) eunit

xref: compile
	$(REBAR) xref skip_deps=true

.dialyzer_plt:
	-dialyzer --output_plt ${@} --build_plt \
           --apps erts kernel stdlib crypto ssl public_key eunit

dialyze: compile .dialyzer_plt
	dialyzer ebin -nn --plt .dialyzer_plt

release: all test
	$(REBAR) release

clean:
	$(REBAR) clean
