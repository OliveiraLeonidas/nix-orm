.PHONY: setup run test clean lint format

# Variáveis
PYTHON = python3
PIP = pip
MODULE = main.py
CACHE_DIR= .cache
TEST_DIR = tests

export PYTHONPYCACHEPREFIX = $(CACHE_DIR)/pycache
export PIP_CACHE_DIR = $(CACHE_DIR)/pip

# Configuração do ambiente
setup:
	$(PIP) install -r requirements.txt
	# docker compose up
	echo "Dependencias instaladas!"

# Execução do programa
run:
	$(PYTHON) $(MODULE)
	echo "Execução finalizada"

# Execução dos testes
test:
	pytest $(TEST_DIR)

# Limpeza de arquivos temporários
clean:
	rm -rf __pycache__
	rm -rf *.pyc
	rm -rf .pytest_cache
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf $(CACHE_DIR)
	docker compose down

# Verificação de estilo
lint:
	flake8 $(MODULE) $(TEST_DIR)
	pylint $(MODULE) $(TEST_DIR)

# Formatação automática
format:
	black $(MODULE) $(TEST_DIR)
	isort $(MODULE) $(TEST_DIR)