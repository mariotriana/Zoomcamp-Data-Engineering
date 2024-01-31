variable "credentials" {
  description = "My Credentials"
  default     = "./keys/my-creds.json"
}

variable "project" {
  description = "Project"
  default     = "weighty-elf-412403"
}
variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BiqQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "weighty-elf-412403-terra-bucket"
}

variable "google_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}