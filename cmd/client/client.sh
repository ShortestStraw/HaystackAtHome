# HS login variables.
hs_key="admin"
hs_secret="admin"
# Sign hs requests and run curl.
function hs_curl() {
# Parse command line.
[ -z "${2}" ] && {
echo "Usage: ${FUNCNAME[0]} <request_type> <hs_url> <data_file>"
return 1
}
# Prepare a signature.
hs_url="${2%/*}"
hs_host="${hs_url##*://}"
hs_date="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
temp="${2#*://}"
temp="${temp#*/}"
temp="${temp%%\?*}"
path="/${temp%%#*}"
# Generate a signature.
if [ "${1}" == "PUT" ]; then
etag="$(md5sum $3 | awk '{print $1}')"
hs_signature="$(echo -en "${1}\n${path}\nx-date:${hs_date}\nAccessKey:${hs_key}\nContent-Type:None\nEtag:${etag}\n" |\
openssl sha256 -hmac ${hs_secret} | awk '{print $2}')"
# Make the request.
curl -H "Host: ${hs_host}" \
-H "x-date: ${hs_date}" \
-H "AccessKey: ${hs_key}" \
-H "Content-Type: None" \
-H "Etag: ${etag}" \
-H "Signature: ${hs_signature}" \
-d "@${3}" \
-X "${1}" \
"${2}"
else
hs_signature="$(echo -en "${1}\n${path}\nx-date:${hs_date}\nAccessKey:${hs_key}\n" |\
openssl sha256 -hmac ${hs_secret} | awk '{print $2}')"
# Make the request.
curl -H "Host:${hs_host}" \
-H "x-date:${hs_date}" \
-H "AccessKey:${hs_key}" \
-H "Signature:${hs_signature}" \
-X "${1}" \
"${2}"
fi
}