all: benchmarks.pdf

benchmarks.pdf: benchmarks.tex
	latexmk -xelatex -shell-escape -pdf $<

.PHONY: clean

clean:
	rm -rf _minted*
	latexmk -C
