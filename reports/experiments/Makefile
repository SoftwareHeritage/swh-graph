all: experiments.pdf

experiments.pdf: experiments.tex
	latexmk -xelatex -shell-escape -pdf $<

.PHONY: clean

clean:
	rm -rf _minted*
	latexmk -C
