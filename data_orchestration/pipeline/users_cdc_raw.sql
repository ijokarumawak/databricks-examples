-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE users_cdc_raw
AS SELECT * FROM cloud_files("${source}", "csv")
