# Calculate MSE
mse <- function(x){
  return(mean((x-1)^2))
}

# Return result of rqs 
resultList <- function(i, n, Tau, mus=mu, sigmas=sigma, betas=beta, ks=k){
  # generate sample
  Data <- generateXY(n, mus, sigmas, betas)
  OrderData <- generateOrderXY(Data)
  
  return(c(globalEstimate(Data[[1]], Data[[2]], Tau),
           oneshotEstimate(Data[[1]], Data[[2]], ks, Tau),
           onestepEstimate(Data[[1]], Data[[2]], ks, Tau),
           globalEstimate(OrderData[[1]], OrderData[[2]], Tau),
           oneshotEstimate(OrderData[[1]], OrderData[[2]], ks, Tau),
           onestepEstimate(OrderData[[1]], OrderData[[2]], ks, Tau)))
}