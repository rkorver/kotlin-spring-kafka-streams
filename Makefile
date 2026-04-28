.PHONY: *

# The first command will be invoked with `make` only and should be `build`
build: ## Standard build, unit-test, and format
	./mvnw verify -Pformat

## Equivalent to `make clean build`
all: clean build

clean: ## Clean the project
	./mvnw clean

format: ## Format the code and pom files
	./mvnw test-compile -DskipTests -Pformat

strict: ## Build and strictly check if dependencies are actually used
	./mvnw test-compile -DskipTests -Ddependency-analyze.strict

update: ## Update versions in the pom files
	./mvnw versions:update-parent versions:update-properties versions:use-latest-versions

yolo: ## Quick build, no tests - use with caution
	./mvnw verify -DskipTests

# This outputs any command in the Makefile. With a short description taken from a ## prefixed command after the command (preferred) or the line above
help: ## Show this help
	@echo "Usage: make <command>"; \
	echo ""; \
	desc=""; \
	while IFS= read -r line; do \
		case "$$line" in \
			'## '*)              desc="$${line#\#\# }" ;; \
			[a-zA-Z_-]*:*'## '*) printf '\033[36m%-20s\033[0m %s\n' "$${line%%:*}" "$${line#*\#\# }"; desc="" ;; \
			[a-zA-Z_-]*:*)       printf '\033[36m%-20s\033[0m %s\n' "$${line%%:*}" "$$desc"; desc="" ;; \
			*)                   desc="" ;; \
		esac; \
	done < $(MAKEFILE_LIST) | sort
