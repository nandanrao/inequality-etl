FILES=$1
RE="([0-9]+)\.csv"
for f in $FILES
do
  if [[ $f =~ $RE ]]
  then
    part="${BASH_REMATCH[1]}"
    awk -i inplace -v d="$part" -F"," 'BEGIN { OFS = "," } {$6=d; print}' $f
    awk -v d="5017" -F"," 'BEGIN { OFS = "," } {$17=d; print}' $f > tmp && mv tmp $f
    sed -i -e '1s/$part/part/' $f
  fi
done
