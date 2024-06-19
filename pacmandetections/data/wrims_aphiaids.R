library(dplyr)
library(stringr)

taxon <- read.csv("../temp/taxon.txt", sep = "\t", header = TRUE, quote = "", na = "")

taxon %>%
  mutate(acceptedNameUsageID = coalesce(acceptedNameUsageID, taxonID)) %>% 
  filter(taxonRank == "Species") %>%
  select(acceptedNameUsageID) %>% 
  mutate(aphiaid = str_extract(acceptedNameUsageID, "[0-9]+$")) %>% 
  select(aphiaid) %>% 
  write.table("wrims_aphiaids.txt", quote = FALSE, row.names = FALSE, col.names = FALSE)
