-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

-- -----------------------------------------------------
-- Schema mydb
-- -----------------------------------------------------
-- -----------------------------------------------------
-- Schema wikipedia
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema wikipedia
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `wikipedia` DEFAULT CHARACTER SET utf8 ;
USE `wikipedia` ;

-- -----------------------------------------------------
-- Table `wikipedia`.`equation`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `wikipedia`.`equation` (
  `eq_id` INT(11) NOT NULL,
  `equation` BLOB NOT NULL,
  PRIMARY KEY (`eq_id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `wikipedia`.`revision`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `wikipedia`.`revision` (
  `page_id` INT(11) NOT NULL,
  `rev_id` INT(11) NOT NULL,
  `date` DATE NOT NULL,
  PRIMARY KEY (`rev_id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `wikipedia`.`revision_equation`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `wikipedia`.`revision_equation` (
  `rev_id` INT(11) NOT NULL,
  `eq_id` INT(11) NOT NULL,
  `count` INT(11) NOT NULL,
  PRIMARY KEY (`rev_id`, `eq_id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
