usage() { echo "Usage: $0 -s <start_block> -e <end_block> -b <batch_size> -p <provider_uri> " 1>&2; exit 1; }

while getopts ":s:e:p:" opt; do
    case "${opt}" in
        s)
            start_block=${OPTARG}
            ;;
        e)
            end_block=${OPTARG}
            ;;
        p)
            provider_uri=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${start_block}" ] || [ -z "${end_block}" ] || [ -z "${provider_uri}" ]; then
    usage
fi
python3 ./export_all.py --start=${start_block} --end=${end_block} --provider-uri="${provider_uri}"
