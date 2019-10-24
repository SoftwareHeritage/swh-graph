all: example.png

%.png: %.dot
	dot -Gdpi=300 -Tpng $< -o $@

.PHONY: clean

clean:
	rm example.png
