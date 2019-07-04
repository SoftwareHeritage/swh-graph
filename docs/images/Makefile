all: compression_steps.png compression_steps.svg

%.png: %.dot
	dot -Gdpi=300 -Tpng $< -o $@

%.svg: %.dot
	dot -Tsvg $< -o $@

.PHONY: clean

clean:
	rm -f compression_steps.png
	rm -f compression_steps.svg
