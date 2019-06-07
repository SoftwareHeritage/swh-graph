all: node_mapping.pdf

node_mapping.pdf: node_mapping.tex
	latexmk -xelatex -shell-escape -pdf $<

.PHONY: clean

clean:
	rm -rf _minted*
	latexmk -C
