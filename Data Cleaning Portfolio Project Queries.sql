/*
Data Cleaning in MySQL for Nashville Housing Data

This script performs standardization of date formats, populates missing address data, 
splits address fields into individual components, updates boolean fields, removes duplicates, 
and deletes unused columns in the NashvilleHousing dataset.
*/

-- ------------------------------------------------------------------------------------------------------------------------

-- Standardize Date Format

-- Convert SaleDate to a standardized DATE format and update the table

ALTER TABLE NashvilleHousing
ADD COLUMN SaleDateConverted DATE;

UPDATE NashvilleHousing
SET SaleDateConverted = STR_TO_DATE(SaleDate, '%m/%d/%Y'); -- Assuming dates are in MM/DD/YYYY format

-- ------------------------------------------------------------------------------------------------------------------------

-- Populate Property Address data

-- Fill missing PropertyAddress entries by matching ParcelID with records that have the address data

UPDATE NashvilleHousing a
JOIN NashvilleHousing b ON a.ParcelID = b.ParcelID
    AND a.UniqueID <> b.UniqueID
SET a.PropertyAddress = COALESCE(a.PropertyAddress, b.PropertyAddress)
WHERE a.PropertyAddress IS NULL;

-- ------------------------------------------------------------------------------------------------------------------------

-- Breaking out Address into Individual Columns (Address, City, State)

-- Split PropertyAddress into PropertySplitAddress (street address) and PropertySplitCity (city name)

ALTER TABLE NashvilleHousing
ADD COLUMN PropertySplitAddress VARCHAR(255),
ADD COLUMN PropertySplitCity VARCHAR(255);

UPDATE NashvilleHousing
SET PropertySplitAddress = SUBSTRING_INDEX(PropertyAddress, ',', 1),
    PropertySplitCity = SUBSTRING_INDEX(SUBSTRING_INDEX(PropertyAddress, ',', -2), ',', 1);

-- Split OwnerAddress into OwnerSplitAddress (street address), OwnerSplitCity (city name), and OwnerSplitState (state)

ALTER TABLE NashvilleHousing
ADD COLUMN OwnerSplitAddress VARCHAR(255),
ADD COLUMN OwnerSplitCity VARCHAR(255),
ADD COLUMN OwnerSplitState VARCHAR(255);

UPDATE NashvilleHousing
SET OwnerSplitAddress = SUBSTRING_INDEX(OwnerAddress, ',', 1),
    OwnerSplitCity = SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', -2), ',', 1),
    OwnerSplitState = SUBSTRING_INDEX(OwnerAddress, ',', -1);

-- ------------------------------------------------------------------------------------------------------------------------

-- Change 'Y' and 'N' to 'Yes' and 'No' in the "SoldAsVacant" field

UPDATE NashvilleHousing
SET SoldAsVacant = CASE
    WHEN SoldAsVacant = 'Y' THEN 'Yes'
    WHEN SoldAsVacant = 'N' THEN 'No'
    ELSE SoldAsVacant
END;

-- ------------------------------------------------------------------------------------------------------------------------

-- Remove Duplicates

-- Use a Common Table Expression (CTE) to identify and remove duplicates based on unique fields such as ParcelID, PropertyAddress, SalePrice, SaleDate, and LegalReference

WITH RowNumCTE AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY ParcelID, PropertyAddress, SalePrice, SaleDate, LegalReference
                              ORDER BY UniqueID) AS row_num
    FROM NashvilleHousing
)
DELETE FROM NashvilleHousing
WHERE UniqueID IN (SELECT UniqueID FROM RowNumCTE WHERE row_num > 1);

-- ------------------------------------------------------------------------------------------------------------------------

-- Delete Unused Columns

ALTER TABLE NashvilleHousing
DROP COLUMN OwnerAddress,
DROP COLUMN TaxDistrict,
DROP COLUMN PropertyAddress,
DROP COLUMN SaleDate;

-- ------------------------------------------------------------------------------------------------------------------------

/* Importing Data using LOAD DATA INFILE (MySQL)

-- In MySQL, the LOAD DATA INFILE statement is used to bulk import data from a CSV file.
-- Ensure that MySQL server permissions allow file access and adjust the file path accordingly.

-- LOAD DATA INFILE '/path/to/Nashville_Housing_Data.csv' 
-- INTO TABLE NashvilleHousing
-- FIELDS TERMINATED BY ',' 
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS; -- Skip header row
*/

