#!/bin/bash
# Bash script do generowania klas C# z CEIDG XSD
# Obsługuje zewnętrzne schematy przez schemaLocation

set -e

# Kolory dla czytelniejszego outputu
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
GRAY='\033[0;90m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Generator klas C# z CEIDG XSD ===${NC}\n"

# 1. Sprawdzenie i instalacja XmlSchemaClassGenerator
echo -e "${GREEN}Sprawdzanie instalacji XmlSchemaClassGenerator...${NC}"
if ! command -v xscgen &> /dev/null; then
    echo -e "${YELLOW}Instalowanie XmlSchemaClassGenerator...${NC}"
    dotnet tool install --global dotnet-xscgen
    export PATH="$PATH:$HOME/.dotnet/tools"
fi

# 2. Przygotowanie katalogów
OUTPUT_DIR="Generated"
CACHE_DIR="SchemaCache"

echo -e "${GREEN}Przygotowywanie katalogów...${NC}"
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"
mkdir -p "$CACHE_DIR"

# 3. Funkcja do pobierania schematów
download_schema() {
    local url=$1
    local file=$2
    
    if [ ! -f "$file" ]; then
        echo -e "  ${GRAY}Pobieranie: $url${NC}"
        if curl -s -f -o "$file" "$url"; then
            echo -e "  ${GREEN}✓ Pobrano: $(basename $file)${NC}"
        else
            echo -e "  ${YELLOW}⚠ Nie można pobrać: $url${NC}"
            echo -e "  ${YELLOW}  Sprawdź połączenie lub pobierz ręcznie${NC}"
            return 1
        fi
    else
        echo -e "  ${CYAN}✓ Używam z cache: $(basename $file)${NC}"
    fi
    return 0
}

# 4. Pobieranie zewnętrznych schematów
echo -e "\n${GREEN}Pobieranie zewnętrznych schematów...${NC}"

# Lista schematów do pobrania
declare -a schemas=(
    "http://crd.gov.pl/xml/schematy/osoba/2009/11/16/osoba.xsd|$CACHE_DIR/osoba.xsd"
    "http://crd.gov.pl/xml/schematy/meta/2009/11/16/meta.xsd|$CACHE_DIR/meta.xsd"
    "http://crd.gov.pl/xml/schematy/struktura/2009/11/16/struktura.xsd|$CACHE_DIR/struktura.xsd"
    "http://www.w3.org/TR/xmldsig-core/xmldsig-core-schema.xsd|$CACHE_DIR/xmldsig-core-schema.xsd"
)

for schema_info in "${schemas[@]}"; do
    IFS='|' read -r url file <<< "$schema_info"
    download_schema "$url" "$file" || true
done

# 5. Kopiowanie głównego pliku CEIDG
echo -e "\n${GREEN}Przygotowywanie głównego pliku XSD...${NC}"
cp "CEIDG-1_10122023.xsd" "$CACHE_DIR/CEIDG-1_10122023.xsd"

# 6. Modyfikacja schemaLocation na lokalne ścieżki
echo -e "${GREEN}Modyfikowanie schemaLocation na lokalne ścieżki...${NC}"
sed -e 's|schemaLocation="http://crd.gov.pl/xml/schematy/osoba/2009/11/16/osoba.xsd"|schemaLocation="osoba.xsd"|g' \
    -e 's|schemaLocation="http://crd.gov.pl/xml/schematy/meta/2009/11/16/meta.xsd"|schemaLocation="meta.xsd"|g' \
    -e 's|schemaLocation="http://crd.gov.pl/xml/schematy/struktura/2009/11/16/struktura.xsd"|schemaLocation="struktura.xsd"|g' \
    -e 's|schemaLocation="http://www.w3.org/TR/xmldsig-core/xmldsig-core-schema.xsd"|schemaLocation="xmldsig-core-schema.xsd"|g' \
    "$CACHE_DIR/CEIDG-1_10122023.xsd" > "$CACHE_DIR/CEIDG-1_10122023_local.xsd"

# 7. Tworzenie pliku mapowania namespace'ów
echo -e "${GREEN}Tworzenie mapowania namespace'ów...${NC}"
cat > "$CACHE_DIR/namespaces.txt" <<EOF
# Mapowanie namespace'ów XML na C#
http://crd.gov.pl/wzor/2023/12/10/ = Ceidg.Wzor
http://crd.gov.pl/xml/schematy/osoba/2009/11/16/ = Ceidg.Osoba
http://crd.gov.pl/xml/schematy/meta/2009/11/16/ = Ceidg.Meta
http://crd.gov.pl/xml/schematy/struktura/2009/11/16/ = Ceidg.Struktura
http://www.w3.org/2000/09/xmldsig# = Ceidg.XmlDSig
EOF

# 8. Generowanie klas C#
echo -e "\n${GREEN}Generowanie klas C#...${NC}"

# Główna komenda generowania
GENERATE_CMD="xscgen \
    --nf \"$CACHE_DIR/namespaces.txt\" \
    --separateFiles \
    --output \"$OUTPUT_DIR\" \
    --nullable \
    --netCore \
    --namespaceHierarchy \
    \"$CACHE_DIR/CEIDG-1_10122023_local.xsd\""

echo -e "${GRAY}Wykonywanie: $GENERATE_CMD${NC}"
eval $GENERATE_CMD

# Alternatywna komenda z unikalnymi nazwami typów (odkomentuj jeśli potrzebne)
# GENERATE_CMD_UNIQUE="xscgen \
#     --nf \"$CACHE_DIR/namespaces.txt\" \
#     --separateFiles \
#     --output \"$OUTPUT_DIR\" \
#     --nullable \
#     --netCore \
#     --namespaceHierarchy \
#     --uniqueTypeNames \
#     \"$CACHE_DIR/CEIDG-1_10122023_local.xsd\""

# 9. Podsumowanie
echo -e "\n${GREEN}=== PODSUMOWANIE ===${NC}"
if [ -d "$OUTPUT_DIR" ]; then
    FILE_COUNT=$(find "$OUTPUT_DIR" -name "*.cs" | wc -l)
    echo -e "${CYAN}Wygenerowano $FILE_COUNT plików C#${NC}"
    echo -e "${CYAN}Lokalizacja: $OUTPUT_DIR${NC}"
    
    echo -e "\n${YELLOW}Struktura katalogów:${NC}"
    find "$OUTPUT_DIR" -type d | while read -r dir; do
        echo -e "  ${GRAY}📁 ${dir}${NC}"
    done
    
    echo -e "\n${YELLOW}Przykładowe pliki (max 10):${NC}"
    find "$OUTPUT_DIR" -name "*.cs" | head -10 | while read -r file; do
        echo -e "  ${GRAY}📄 ${file}${NC}"
    done
else
    echo -e "${RED}BŁĄD: Nie udało się wygenerować klas${NC}"
    exit 1
fi

echo -e "\n${GREEN}=== UŻYCIE W PROJEKCIE ===${NC}"
echo -e "${CYAN}1. Skopiuj katalog 'Generated' do swojego projektu"
echo -e "2. Dodaj do .csproj:"
echo -e "   <ItemGroup>"
echo -e "     <Compile Include=\"Generated/**/*.cs\" />"
echo -e "   </ItemGroup>"
echo -e "3. Używaj w kodzie:"
echo -e "   using Ceidg.Wzor;"
echo -e "   using Ceidg.Osoba;"
echo -e "   // itd.${NC}"

echo -e "\n${GREEN}=== ROZWIĄZYWANIE PROBLEMÓW ===${NC}"
echo -e "${YELLOW}Jeśli wystąpią konflikty nazw typów:${NC}"
echo -e "1. Odkomentuj linię z --uniqueTypeNames w skrypcie"
echo -e "2. Lub użyj bardziej szczegółowego mapowania per-plik:"
echo -e "   xscgen -n \"|osoba.xsd=Ceidg.Osoba.Types\" ..."
