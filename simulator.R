# load packages
library(MASS)

generateSigma <- function(P){
  # Generate sigma matrix of dependent variables
  
  # Args:
  # P - demension of dependent variables
  
  sigma <- diag(0, P)
  for(j1 in 1:P)
  {
    for(j2 in 1:P)
      sigma[j1,j2] <- 0.5^(abs(j1-j2))
  }
  
  return(sigma)
}


generateXY <- function(N, mu, sigma, beta){
  # Generate simulated data
  
  # Args:
  # N - number of observations
  # mu - mean of dependent variables
  # sigma - sigma of dependent variables
  # beta - true value of beta
  
  # generate data
  X <- mvrnorm(N,mu,sigma)
  error <- rcauchy(N)
  Y <- X %*% beta + error
  
  return(list(X, Y))
}

generateOrderXY <- function(Data){
  # Generate simulated serial dependent data
  
  # Args:
  # Data - data generated using generateXY
 
  X <- Data[[1]]
  Y <- Data[[2]]
  
  # order data by row sum
  Z <- apply(X, 1, sum) # 为什么原来代码写的是（222111）
  rk <- order(Z)
  
  X <- X[rk,]
  Y <- Y[rk]

  return(list(X, Y))
}

