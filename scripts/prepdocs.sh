#!/bin/bash
echo ""
echo "Loading azd .env file from current environment"
echo ""

while IFS='=' read -r key value; do
    value=$(echo "$value" | sed 's/^"//' | sed 's/"$//')
    export "$key=$value"
done <<EOF
$(azd env get-values)
EOF

echo 'Creating python virtual environment "scripts/.venv"'
python3 -m venv scripts/.venv

echo 'Installing dependencies from "requirements.txt" into virtual environment'
./scripts/.venv/bin/python -m pip install -r scripts/requirements.txt

echo 'Running "prepdocs.py"'


# Construct the file-index pairs
declare -a FILES


declare -A DIRECTORY_TO_INDEX
DIRECTORY_TO_INDEX["energy"]="energy"
DIRECTORY_TO_INDEX["greenminerals"]="green-minerals"
DIRECTORY_TO_INDEX["sustagric"]="sust-agric"
DIRECTORY_TO_INDEX["climatefinancing"]="climate-financing"
DIRECTORY_TO_INDEX["adaptation"]="adaptation"
DIRECTORY_TO_INDEX["sustinfrastructure"]="infrastructure"
DIRECTORY_TO_INDEX["naturalcapital"]="natural-capital"

declare -A DIRECTORY_TO_CONTAINER
DIRECTORY_TO_CONTAINER["energy"]="energy"
DIRECTORY_TO_CONTAINER["greenminerals"]="green-minerals"
DIRECTORY_TO_CONTAINER["sustagric"]="sust-agric"
DIRECTORY_TO_CONTAINER["climatefinancing"]="climate-financing"
DIRECTORY_TO_CONTAINER["adaptation"]="adaptation"
DIRECTORY_TO_CONTAINER["sustinfrastructure"]="infrastructure"
DIRECTORY_TO_CONTAINER["naturalcapital"]="natural-capital"

for dir in "${!DIRECTORY_TO_INDEX[@]}"; do
    for file in ./data/$dir/*; do
        FILES+=( "$file:${DIRECTORY_TO_INDEX[$dir]}:${DIRECTORY_TO_CONTAINER[$dir]}" )
    done
done

./scripts/.venv/bin/python ./scripts/prepdocs.py "${FILES[@]}" --storageaccount "$AZURE_STORAGE_ACCOUNT" --searchservice "$AZURE_SEARCH_SERVICE" --openaiservice "$AZURE_OPENAI_SERVICE" --openaideployment "$AZURE_OPENAI_EMB_DEPLOYMENT" --tenantid "$AZURE_TENANT_ID" --localpdfparser -v