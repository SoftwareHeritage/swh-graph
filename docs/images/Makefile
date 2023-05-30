GENERATED_IMAGES = \
	compression_steps.png compression_steps.svg \
	example-dataset.png example-dataset.svg

all: $(GENERATED_IMAGES)

%.png: %.dot
	dot -Gdpi=150 -Tpng $< -o $@

%.svg: %.dot
	dot -Tsvg $< -o $@

.PHONY: clean

clean:
	rm -f $(GENERATED_IMAGES)
