#!/bin/bash
echo "Waiting for Ollama to start..."
until curl -s http://localhost:11434/api/tags > /dev/null 2>&1; do
    sleep 2
done
echo "Pulling Mistral 7B model..."
curl -s http://localhost:11434/api/pull -d '{"name": "mistral:7b"}'
echo "Model pull complete."
