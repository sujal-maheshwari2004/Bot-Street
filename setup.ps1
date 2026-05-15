# Run from the Bot-Street repo root

$PROJECT = "bot-street"
$REGION  = "us-central1"
$TAG     = "v2"
$IMAGE   = "$REGION-docker.pkg.dev/$PROJECT/bot-street/bot-street:$TAG"

# Auth docker with GCP
gcloud auth configure-docker "$REGION-docker.pkg.dev"

# Build
docker build -t $IMAGE .

# Push
docker push $IMAGE

# Update all deployment YAMLs
$OLD = "us-central1-docker.pkg.dev/bot-street/bot-street/bot-street:v1"
$NEW = $IMAGE

$files = Get-ChildItem -Path "k8s\deployments" -Filter "*.yaml"
foreach ($file in $files) {
    $content = Get-Content $file.FullName -Raw
    if ($content -match [regex]::Escape($OLD)) {
        $content = $content -replace [regex]::Escape($OLD), $NEW
        Set-Content $file.FullName $content
        Write-Host "Updated: $($file.Name)" -ForegroundColor Green
    }
}

Write-Host ""
Write-Host "Image pushed and YAMLs updated to: $IMAGE" -ForegroundColor Green
Write-Host "Now run: kubectl rollout restart deployment -n bot-street" -ForegroundColor Cyan