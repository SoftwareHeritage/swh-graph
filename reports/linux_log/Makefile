all: linux_log.pdf

linux_log.pdf: linux_log.tex
	latexmk -xelatex -shell-escape -pdf $<

.PHONY: clean

clean:
	rm -rf _minted*
	latexmk -C
