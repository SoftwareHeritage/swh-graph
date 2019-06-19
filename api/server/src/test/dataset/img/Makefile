all: graph.png

%.png: %.dot
	dot -Gdpi=300 -Tpng $< -o $@

.PHONY: clean

clean:
	rm graph.png
