GENERATED_IMAGES = \
	compression_steps.png compression_steps.svg \
	example-dataset.png example-dataset.svg

all: $(GENERATED_IMAGES)

compression_steps.dot:
	python3 -c "print(__import__('swh.graph.luigi.compressed_graph').graph.luigi.compressed_graph._make_dot_diagram())" > $@

%.png: %.dot
	dot -Gdpi=150 -Tpng $< -o $@

%.svg: %.dot
	dot -Tsvg $< -o $@

.PHONY: clean compression_steps.dot

clean:
	rm -f $(GENERATED_IMAGES)
