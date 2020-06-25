@app
init

@http
get /
get /create-part-v2
get /parse-file-v2

@tables
files
  url *String

@indexes
files
  dataset *String

@aws
profile default
timeout 600
