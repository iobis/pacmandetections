library(dplyr)
library(stringr)

taxon <- read.csv("../../temp/taxon.txt", sep = "\t", header = TRUE, quote = "", na = "")

taxon %>%
  mutate(acceptedNameUsageID = coalesce(acceptedNameUsageID, taxonID)) %>% 
  filter(taxonRank == "Species") %>%
  select(acceptedNameUsageID, name = acceptedNameUsage) %>% 
  mutate(aphiaid = str_extract(acceptedNameUsageID, "[0-9]+$")) %>% 
  select(aphiaid, name) %>%
  filter(!is.na(name)) %>%
  distinct() %>%
  write.table("wrims_aphiaids.txt", quote = FALSE, row.names = FALSE, col.names = FALSE, sep = "\t")
