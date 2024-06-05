SELECT * FROM LAYOFFS;
-- 1. REMOVE DUPLICATES IF ANY
-- 2. STANDARDIZED DATA
-- 3. NULL VALUES/ BLANK VALUES
-- 4. REMOVE UNNECESSARY COLUMNS
CREATE TABLE LAYOFFS_STAGING LIKE LAYOFFS;
SELECT * FROM LAYOFFS_STAGING;
INSERT LAYOFFS_STAGING SELECT * FROM LAYOFFS;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY COMPANY,INDUSTRY,TOTAL_LAID_OFF,PERCENTAGE_LAID_OFF,`DATE`) AS ROW_NUM
FROM LAYOFFS_STAGING;

WITH DUBLICATE_CTE AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY COMPANY,LOCATION,INDUSTRY,TOTAL_LAID_OFF,PERCENTAGE_LAID_OFF,STAGE,`DATE`,COUNTRY,FUNDS_RAISED_MILLIONS) AS ROW_NUM
FROM LAYOFFS_STAGING
)
DELETE FROM DUBLICATE_CTE
WHERE ROW_NUM>1;

SELECT * FROM layoffs_staging2;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2 
SELECT *,
ROW_NUMBER() OVER(PARTITION BY COMPANY,LOCATION,INDUSTRY,TOTAL_LAID_OFF,PERCENTAGE_LAID_OFF,STAGE,`DATE`,COUNTRY,FUNDS_RAISED_MILLIONS) AS ROW_NUM
FROM LAYOFFS_STAGING;
 
DELETE FROM layoffs_staging2 WHERE ROW_NUM>1;

SELECT * FROM layoffs_staging2;

-- STANDARDIZING DATA

SELECT TRIM(COMPANY) FROM layoffs_staging2; 
UPDATE layoffs_staging2
SET COMPANY = TRIM(COMPANY);

SELECT distinct location FROM layoffs_staging2 ORDER BY 1;

SELECT location
FROM layoffs_staging2
WHERE location REGEXP '[^a-zA-Z0-9 .]';

SELECT distinct country FROM layoffs_staging2 order by 1 ;
SELECT * FROM layoffs_staging2 where country ='united states'; -- WHERE INDUSTRY LIKE 'CRYPTO%';

UPDATE layoffs_staging2
SET INDUSTRY='Crypto'
WHERE INDUSTRY LIKE 'CRYPTO%';


UPDATE layoffs_staging2
SET COUNTRY='United States'
WHERE COUNTRY LIKE 'United states%';

SELECT `DATE`, str_to_date(`DATE`, '%m/%d/%Y') FROM LAYOFFS_STAGING2;

UPDATE layoffs_staging2
SET `DATE`= str_to_date(`DATE`,'%m/%d/%Y');

SELECT `DATE` FROM layoffs_staging2;

ALTER TABLE layoffs_staging2 MODIFY COLUMN `date` DATE;

SELECT * FROM layoffs_staging2 WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

SELECT * FROM layoffs_staging2 WHERE industry IS NULL OR industry = '';

select * from layoffs_staging2 where company= 'Airbnb';

SELECT * FROM layoffs_staging2 T1
JOIN layoffs_staging2 T2 
ON T1.company=T2.company AND T1.location = 	T2.location
WHERE (T1.industry IS NULL OR T1.industry ='')
AND T2.industry IS NOT NULL; 

UPDATE layoffs_staging2
SET INDUSTRY= NULL
WHERE INDUSTRY= '';

UPDATE layoffs_staging2 T1
JOIN layoffs_staging2 T2 
     on T1.COMPANY = T2.COMPANY
SET T1.INDUSTRY=T2.INDUSTRY
WHERE (T1.INDUSTRY IS NULL OR T1.INDUSTRY = '')
AND T2.INDUSTRY IS NOT NULL;
SELECT * FROM layoffs_staging2 WHERE industry IS NULL OR industry='';
SELECT * FROM layoffs_staging2 WHERE company like 'Bally%';

SELECT * FROM layoffs_staging2 
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

DELETE 
FROM layoffs_staging2 
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; 