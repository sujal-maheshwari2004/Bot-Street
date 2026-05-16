# Run from the Bot-Street (backend) folder

$PROJECT = "bot-street"
$REGION  = "us-central1"
$IMAGE   = "$REGION-docker.pkg.dev/$PROJECT/bot-street/bot-street:v4"

# Build and push
gcloud auth configure-docker "$REGION-docker.pkg.dev"
docker build -t $IMAGE .
docker push $IMAGE

# Roll out new image to every backend deployment
foreach ($deploy in @("api", "price-feed", "engine", "ledger", "bots", "candles", "circuit", "sentiment")) {
    kubectl set image deployment/$deploy `
        $deploy=$IMAGE `
        -n bot-street
    Write-Host "Updated $deploy" -ForegroundColor Cyan
}

Write-Host ""
Write-Host "Backend deployed! Watching pods..." -ForegroundColor Green
kubectl get pods -n bot-street -w