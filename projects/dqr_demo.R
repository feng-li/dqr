# clear working space
rm(list = ls())

# set r to show 200 digits
options(scipen = 200)

# source all functions
source("simulator.R")
source("estimator.R")
source("utils.R")

# load packages
library(tidyverse)
library(MASS)
library(plyr)
library(quantreg)

# set random seed
set.seed(2019)

# set params
N <- c(200, 500, 800 ,1000 ,2000 ,4000 ,6000 ,10000,
       12000, 15000, 18000, 20000, 25000, 30000) # sample size
Tau <- c(0.1, 0.5, 0.9)                          # list of quantiles
B <- 1000                                        # number of replications
k <- 10                                          # number of machines

# record MSE
MSE <- c() 

# generate mu and sigma for simulated x variables
mu <- rep(0, 6)
sigma <- generateSigma(6)

# true beta values
beta <- rep(1, 6)

# Calculate MSE for all n and tau
# iterate through all n and tau
for (n in N){
  for (tau in Tau){
    # ResultList returns oneshot & pilot & onestep estimates for 
    # a specific n and tau; for each n and tau
    # 1000 replications are applied
    temp <- sapply(1:B, resultList, n=n, Tau=tau)
    # The MSE function calculate mse for each beta estimate
    # and we calcualte the mean of each 1000 beta estimates
    MSE <- rbind(MSE, c(apply(temp, 1, mse), n, tau))
  }
}

# Calculate RMSE

# First step is to calculate RMSE wrt a single beta estimate
# MSE[,1:6] is the global estimation of random data
# MSE[,25:30] is the global estimation of ordered data
MSE[,7:12] <- MSE[,7:12]/MSE[,1:6]
MSE[,13:18] <- MSE[,13:18]/MSE[,1:6]
MSE[,19:24] <- MSE[,19:24]/MSE[,1:6]
MSE[,31:36] <- MSE[,31:36]/MSE[,25:30]
MSE[,37:42] <- MSE[,37:42]/MSE[,25:30]
MSE[,43:48] <- MSE[,43:48]/MSE[,25:30]

# Second step is to calculate a mean RMSE of beta_1 to beta_6
# for each n and tau
RMSE <- cbind(apply(MSE[,7:12],1,mean), apply(MSE[,13:18],1,mean),
              apply(MSE[,19:24],1,mean), apply(MSE[,31:36],1,mean),
              apply(MSE[,37:42],1,mean), apply(MSE[,43:48],1,mean),
              MSE[,49:50])

# generate a new column to distinguish result of random data and order data
RMSE <- rbind(cbind(RMSE[-c(1:6),c(1:3,7,8)],1), cbind(RMSE[-c(1:6),c(4:8)],0))

# update colnames of RMSE
colnames(RMSE) <- c('One Shot', 'Pilot Esti.','One Step', 'n', 'tau', 'random')

# save RMSE data
save(RMSE, file = "RMSE.RData")

# Plot
RMSE %>%
  # matrix to data frame
  as.data.frame() %>%
  # set random and tau as factor
  mutate(random = factor(random, levels = c(1,0)),
         tau = factor(tau, levels = c(0.1,0.5,0.9))) %>%
  # gather by type of estimation
  gather(key = Method, value = RMSE, -n, -tau, -random) %>%
  # plot RMSE by n and type of estimation
  ggplot(aes(x = n, y = RMSE, color = Method)) +
    geom_line(aes(linetype = Method)) +
    geom_point(aes(shape = Method)) +
    # distributed plots by random/non-random and tau value
    facet_grid(tau~random, labeller=labeller(random = c("1" = "Random", 
                                                        "0" = "Non Random"),
                                             tau = c("0.1" = "Tau = 0.1",
                                                     "0.5" = "Tau = 0.5",
                                                     "0.9" = "Tau = 0.9"))) +
    # setting of axis and theme
    scale_x_continuous(expand = c(0,0)) +
    scale_y_continuous(expand = c(0,1)) +
    theme(axis.text = element_text(face = "bold",size=10), 
          axis.title = element_text(face = "bold",size=12),
          panel.background = element_rect(fill = "transparent"),
          panel.border=element_rect(fill='transparent', 
                                    color='transparent'),
          panel.grid.major = element_line(color = "grey"),
          strip.background = element_blank(),
          legend.key = element_blank(),
          axis.line = element_line(color = "black")) +
    # save plot
    ggsave(filename = "RMSE.png", width = 8, height = 7)