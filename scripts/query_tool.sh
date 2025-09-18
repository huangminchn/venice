#!/bin/bash

# Only works on files for now since. It fails for special directories "." and "/" since "basename" and "dirname" returns
# the same value
function real_path() {
  local dir_path=$(cd "$(dirname "$1")" && pwd)
  local file_name=$(basename "$1")
  echo "$dir_path/$file_name"
}

function is_compatible () {
  "$@" >/dev/null 2>&1
}

function is_compatible_date_f() {
    is_compatible date -j -f "%b %d %X %Y %Z" "Jan 1 00:00:00 2022 GMT"
}

function is_compatible_date_d() {
    is_compatible date -d "Jan 1 00:00:00 2022 GMT"
}

is_compatible_date_f
useBsdDate=$?

is_compatible_date_d
useGnuDate=$?

if [ $useBsdDate -eq 1 ] && [ $useGnuDate -eq 1 ] ; then
  echo "ERROR: Cannot identify which flavor of 'date' command to use. Please report the error."
  exit 1
fi

# GNU: date -d "Apr 29 16:43:30 2022 GMT" +"%s"
# BSD: date -j -f "%b %d %X %Y %Z" "Apr 29 16:43:30 2022 GMT" +"%s"
function parse_openssl_date_to_epoch_seconds() {
  if [ $useGnuDate -eq 0 ]; then
    date -d "$1" +"%s"
  elif [ $useBsdDate -eq 0 ]; then
    date -j -f "%b %d %X %Y %Z" "$1" +"%s"
  fi
}

script_real_path=$(real_path "${BASH_SOURCE[0]}") # Absolute path
base_dir=$(dirname "$script_real_path")

function cert_has_enough_time() {
  cert=$1
  password=$2

  not_after=$(2>/dev/null openssl pkcs12 -in "$cert" -clcerts -nodes -passin "pass:$password" | openssl x509 -noout -enddate | cut -d= -f2)
  expiration_time=$(parse_openssl_date_to_epoch_seconds "$not_after")
  current_time=$(date +%s)
  remaining_time=$((expiration_time - current_time))

  # Cert must be valid for a small buffer zone so that during execution of this program the certificate will remain valid.
  if (( remaining_time < 30 )); then    # in seconds
    return 1
  else
    return 0
  fi
}


if [ $# -gt 5 ] || [ $# -lt 3 ]; then
  echo "  Usage: $0 <fabric> <store_name> <key_string> [is_vson_store] [is_server_direct]"
  echo "  Example: $0 ei-ltx1 LeapContentRecommendationTest '{\"contractId\":2660,\"memberId\":1,\"modelVersionId\":\"testModel\",\"source\":\"NEARLINE\"}'"
  echo "  Example (server-direct): $0 ei-ltx1 LeapContentRecommendationTest '[\"key1\",,,,,\"key2\"]' false true"
  exit 1
else
  # Initialize essential variables
  fabric="$1"
  store_name="$2"
  key_string="$3"
  if [ $# -gt 3 ]; then
    is_vson_store="$4"
  else
    is_vson_store="false"
  fi
  if [ $# -gt 4 ]; then
    is_server_direct="$5"
  else
    is_server_direct="false"
  fi
fi

# Use local Venice client jar
jar_file="/Users/mnhuang/Documents/Github/venice/clients/venice-client/build/libs/venice-client-all.jar"

if [[ ! -e "$jar_file" ]]; then
  echo "ERROR: Cannot locate Venice client jar at $jar_file"
  echo "Please build the project first by running: ./gradlew :clients:venice-client:shadowJar"
  exit 1
fi


# Prepare SSL configuration file
ssl_config_file="${base_dir}/ssl.config"
ssl_configs="ssl.enabled=true
ssl.keystore.type=PKCS12
ssl.keystore.password=work_around_jdk-6879539
ssl.keystore.location=${base_dir}/identity.p12
ssl.truststore.password=changeit
ssl.truststore.location=/etc/riddler/cacerts"
echo -n "$ssl_configs" > "$ssl_config_file"


# Validate certificate
if ! grep -q "ssl.keystore.password=" "$ssl_config_file"; then
  echo "ERROR: $ssl_config_file does not contain ssl.keystore.password"
  exit 1
elif ! grep -q "ssl.keystore.location=" "$ssl_config_file"; then
  echo "ERROR: $ssl_config_file does not contain ssl.keystore.location"
  exit 1
elif ! grep -q "ssl.keystore.type=" "$ssl_config_file"; then
  echo "ERROR: $ssl_config_file does not contain ssl.keystore.type"
  exit 1
elif ! grep -q "ssl.truststore.location=" "$ssl_config_file"; then
  echo "ERROR: $ssl_config_file does not contain ssl.truststore.location"
  exit 1
elif ! grep -q "ssl.truststore.password=" "$ssl_config_file"; then
  echo "ERROR: $ssl_config_file does not contain ssl.truststore.password"
  exit 1
elif ! grep -q "ssl.enabled=" "$ssl_config_file"; then
  echo "ERROR: $ssl_config_file does not contain ssl.enabled"
  exit 1
fi

keystore_password=$(grep "ssl.keystore.password=" "$ssl_config_file" | cut -d= -f2-)
keystore_path=$(grep "ssl.keystore.location=" "$ssl_config_file" | cut -d= -f2-)
keystore_real_path=$(real_path "$keystore_path")
truststore_path=$(grep "ssl.truststore.location=" "$ssl_config_file" | cut -d= -f2-)

if [[ "$keystore_path" != "$keystore_real_path" ]]; then
  echo "ERROR: Please use absolute path for $keystore_path"
  exit 1
elif [[ ! -e "$truststore_path" ]]; then
  echo "ERROR: $truststore_path does not exist"
  exit 1
elif [[ ! -e "$keystore_path" ]]; then
  # $keystore_path does not exist
  echo
  echo "Creating certificate ..."
  if ! id-tool grestin sign -o "$base_dir"; then
    echo "ERROR: Failed to create certificate"
    exit 1
  fi
elif ! cert_has_enough_time "$keystore_path" "$keystore_password"; then
  echo "Your certificate $keystore_path has expired. Creating new certificate ..."
  if ! id-tool grestin sign -o "$base_dir"; then
    echo "ERROR: Failed to renew certificate"
    exit 1
  fi
fi

if [[ ! -e "${base_dir}/identity.p12" ]]; then
  echo "ERROR: Cannot locate identity.p12 keystore file. Please report the error."
  exit 1
elif ! cert_has_enough_time "$keystore_path" "$keystore_password"; then
  echo "ERROR: Cannot renew expired certificate. Please report the error."
  exit 1
fi

echo "Gathering information from remote ..."

# Discover cluster for store
d2_result=$(2>.stderr curli --no-log --force-insecure-d2 --fabric "$fabric" "d2://venice-discovery/discover_cluster/$store_name")
error=$(<.stderr)

if [[ "$error" == *"[ERROR]"* ]]; then
  echo "$error"
  exit 1
fi

if [[ "$d2_result" == *"doesn't exist"* ]]; then
  echo "ERROR: Invalid store name $store_name"
  exit 1
elif [[ -z "$d2_result" ]]; then
  echo "ERROR: D2 returned nothing"
  exit 1
fi

d2_cluster=$(echo "$d2_result" | grep "d2Service" | cut -d: -f2 | tr -d ' ",')

if [[ -z "$d2_cluster" ]]; then
  echo "ERROR: Cannot determine D2 cluster. Please try again. If the issue persists, please report the error to Venice team."
  echo "D2 result: $d2_result"
  exit 1
fi

# Choose a Venice router that supports https
router_url=$(2>/dev/null curli --no-log --force-insecure-d2 --fabric "$fabric" "d2://d2Clusters/$d2_cluster" | grep "https://" | head -1 | cut -d: -f2- | tr -d ' ",')

if [[ -z $router_url ]]; then
  echo "ERROR: Cannot determine router URL. Please try again. If the issue persists, please report the error to Venice team."
  exit 1
fi

echo "Checking local environment ..."

# Prepare for invocation
# Check if java exists
if ! command -v java > /dev/null; then
  # Cannot find java in default $PATH. Give it a second chance.
  # The following works on Linux only.
  jdk_dir=$(find '/export/apps/jdk/' -type d -name 'JDK-*' | sort -V | tail -1) # Latest compatible version
  if [[ -z $jdk_dir ]]; then
    # Most likely running on a Mac.
    echo 'ERROR: Cannot find "java" from PATH'
    exit 1
  fi
  PATH="$jdk_dir/bin:$PATH"
fi

echo
if [[ "$is_server_direct" == "true" ]]; then
  echo "Will send a server-direct request to $router_url for store $store_name with key string: $key_string"
else
  echo "Will send a router request to $router_url for store $store_name with key string: $key_string"
fi

router_hostname_and_port=$(echo "$router_url" | sed 's/https:\/\///')
router_hostname=$(echo "$router_hostname_and_port" | cut -d: -f1)
router_port=$(echo "$router_hostname_and_port" | cut -d: -f2)

if ! echo > "/dev/tcp/$router_hostname/$router_port"; then
  echo
  echo "ERROR: Failed to establish connection to Venice router $router_hostname"
  echo "You must run this tool in $fabric"
  exit 1
fi

java -jar "$jar_file" "$store_name" "$key_string" "$router_url" "$is_vson_store" "$ssl_config_file" "$is_server_direct" 2>.stderr
query_command_status=$?
error_message=$(<.stderr)
echo "$error_message"
if [ $query_command_status -ne 0 ] && [[ $error_message = *"Error: Invalid or corrupt jarfile"* ]]; then
  installed_java_version="$(java -version 2>&1 | head -n 1 | awk -F '"' '{print $2}')"
  echo "Currently configured JRE version ($installed_java_version) may be outdated. Please update JAVA_HOME and try again."
fi
