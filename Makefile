# -------- project-wide vars (override in CI) -----------------
PROJECT_ID      ?= healbe-1071
REGION          ?= us-central1
IMAGE_TAG       ?= 1.0.0
IMAGE           := gcr.io/$(PROJECT_ID)/nps-intercom-pipeline:$(IMAGE_TAG)
TEMPLATE_GCS    ?= gs://healbe_analytics/templates/nps_intercom_pipeline.json

# ---------------- targets ------------------------------------
.PHONY: build push template test lint

build:
	docker build -t $(IMAGE) --build-arg PROJECT_ID=$(PROJECT_ID) .

push: build
	docker push $(IMAGE)

template: push
	gcloud dataflow flex-template build $(TEMPLATE_GCS) \
		--image=$(IMAGE) \
		--metadata-file=metadata.json \
		--sdk-language=PYTHON

test:
	python -m pytest -q || true

lint:
	ruff nps_intercom.py
	black --check nps_intercom.py 