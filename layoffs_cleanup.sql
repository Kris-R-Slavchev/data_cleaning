USE world_layoffs;

SELECT * FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging SELECT * FROM layoffs ;


SELECT * FROM layoffs_staging;


-- Identify duplicate rows by assigning row numbers

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off , `date`) AS row_num
FROM layoffs_staging; 


-- Find duplicates using CTE (keep row_num > 1)

WITH duplicate_cte AS 
(
    SELECT *,
           ROW_NUMBER() OVER(
               PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
           ) AS row_num
    FROM layoffs_staging 
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


-- Retrieve records for a specific company

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';


-- Re-run duplicate check for verification

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off , `date`) AS row_num
FROM layoffs_staging; 


-- Create a new table with row numbers

WITH duplicate_cte AS 
(
    SELECT *,
           ROW_NUMBER() OVER(
               PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
           ) AS row_num
    FROM layoffs_staging 
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- 

CREATE TABLE layoffs_staging2 AS
SELECT * 
FROM (
	SELECT *,
		ROW_NUMBER() OVER(
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
           ) AS row_num
    FROM layoffs_staging
) subquery;


-- Verify data in the new table

SELECT * 
FROM layoffs_staging2;


DELETE FROM layoffs_staging2
WHERE row_num > 1;


-- Delete duplicate rows (keep row_num = 1)

DROP TABLE layoffs_staging;

ALTER TABLE layoffs_staging2 RENAME TO layoffs_staging;


-- Standardizing companies

SELECT company, TRIM(company)
FROM layoffs_staging;


UPDATE layoffs_staging
SET company = TRIM(company);

SELECT company
FROM layoffs_staging;


SELECT DISTINCT industry
FROM layoffs_staging
ORDER BY 1;


SELECT industry
FROM layoffs_staging
WHERE industry LIKE '%Crypto%';


UPDATE layoffs_staging
SET industry = 'Crypto'
WHERE industry LIKE '%Crypto%';

-- Standadizing locations

SELECT DISTINCT location
FROM layoffs_staging
ORDER BY 1;


SELECT location 
FROM layoffs_staging 
WHERE location LIKE ' %'
OR location LIKE '% '
OR location LIKE '%  %';


SELECT DISTINCT country 
FROM layoffs_staging
ORDER BY 1;

UPDATE layoffs_staging
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Fixing date column

SELECT `date`
FROM layoffs_staging;


SELECT `date`
FROM layoffs_staging
WHERE STR_TO_DATE(`date`, '%m/%d/%Y') IS NULL
AND `date` IS NOT NULL;


UPDATE layoffs_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')
WHERE `date` IS NOT NULL AND `date` <> 'NULL' AND TRIM(`date`) <>;

UPDATE layoffs_staging
SET `date` = NULL
WHERE `date` = 'NULL' OR `date` = '';

ALTER TABLE layoffs_staging 
MODIFY COLUMN `date` DATE;


SELECT *
FROM layoffs_staging
WHERE industry IS NULL 
OR industry = '';


SELECT *
FROM layoffs_staging
WHERE company = 'Airbnb';




SELECT col1.company, col1.industry , col2.industry
FROM layoffs_staging col1
JOIN layoffs_staging col2
	ON col1.company = col2.company
	AND col1.location = col2.location
WHERE (col1.industry IS NULL OR col1.industry = '')
AND col2.industry IS NOT NULL; 


UPDATE layoffs_staging col1
JOIN layoffs_staging col2
	ON col1.company = col2.company 
SET col1.industry = col2.industry
WHERE (col1.industry IS NULL OR col1.industry = '')
AND col2.industry IS NOT NULL; 

-- Removing columns and rows we need to.

SELECT * 
FROM layoffs_staging 
WHERE total_laid_off = 'NULL'
AND  percentage_laid_off  = 'NULL';


DELETE
FROM layoffs_staging 
WHERE total_laid_off = 'NULL'
AND  percentage_laid_off  = 'NULL';


SELECT * 
FROM layoffs_staging;


ALTER TABLE layoffs_staging 
DROP COLUMN row_num;


