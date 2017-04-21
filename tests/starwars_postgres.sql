-- My custom Star Wars character database for testing fun...
-- Star Wars Test DataSet v1.0.1 for SQLite3
--
-- Copyright 2017, Matt McElheny <mattmc3 at google's standard email domain dotcom>
-- License: Creative Commons v4.0
--          https://creativecommons.org/licenses/by/4.0/legalcode
--          https://wiki.creativecommons.org/wiki/Data
--
-- NOTE: Please remember that this is merely a *test* dataset. As such we need
-- to rely on the contents of this file remaining constant over being accurate
-- and complete! If the contents of this dataset offend your nerd sensibilities
-- in any way, stay calm and follow this simple axiom:
--
--     **Change it the way you want it to be IN A TEST, not here.**
--
-- 1.) If you find yourself thinking: "Does Leia really have the Force if she
--     never learned to be a Jedi???"... then: you should write a test of the
--     UPDATE function to make it the way you want it.
-- 2.) If you find yourself thinking: "But you forgot <character x>!? (S)he is
--     the best! How could you forget <character x>?"... then: you should write
--     a test of the INSERT function add that character. Unless yousa wanna add
--     _that_ character, in which case turn in your nerd creds. Meesa done
--     here. There's no hope for you. If you're missing Wedge Antilles or
--     Porkins, well done!
-- 3.) If you find some other nit to pick, post your own dataset to a site like
--     https://www.kaggle.com/datasets. Devs like me sure could use free, light,
--     easy to grok test datasets for use in projects like this. And don't
--     forget to license via Creative Commons or another permissive license.
--     My sincerest thanks!

DROP TABLE IF EXISTS characters;
DROP TABLE IF EXISTS movies;

CREATE TABLE movies (
    id SERIAL PRIMARY KEY NOT NULL
    ,name VARCHAR(255)
    ,episode VARCHAR(255)
    ,director VARCHAR(255)
    ,release_year INT
    ,chronology INT
);

CREATE TABLE characters (
    id SERIAL PRIMARY KEY NOT NULL
    ,name VARCHAR(255) NOT NULL
    ,sex VARCHAR(255)
    ,character_type VARCHAR(255)
    ,allegiance VARCHAR(255)
    ,first_appeared_movie_id INT REFERENCES movies(id)
    ,has_force BOOLEAN
    ,died_in_movie_id INT REFERENCES movies(id)
);

INSERT INTO movies
(name, episode, director, release_year, chronology)
VALUES
 ('Star Wars (A New Hope)', 'IV', 'George Lucas', 1977, 5)
,('The Empire Strikes Back', 'V', 'Irvin Kershner', 1980, 6)
,('Return of the Jedi', 'VI', 'George Lucas', 1983, 7)
,('Star Wars: Episode I - The Phantom Menace', 'I', 'George Lucas', 1999, 1)
,('Star Wars: Episode II - Attack of the Clones', 'II', 'George Lucas', 2002, 2)
,('Star Wars: Episode III - Revenge of the Sith', 'III', 'George Lucas', 2005, 3)
,('Star Wars: The Force Awakens', 'VII', 'J.J. Abrams', 2015, 8)
,('Rogue One: A Star Wars Story', NULL, 'Gareth Edwards', 2016, 4)
,('Star Wars: The Last Jedi', 'VIII', 'Rian Johnson', 2017, 9)
;

-- 42 of them!
INSERT INTO characters
(name, sex, character_type, allegiance, first_appeared_movie_id,
 has_force, died_in_movie_id)
VALUES
-- Star Wars
 ('Luke Skywalker', 'M', 'Human', 'The Rebel Alliance', 1, True, NULL)
,('Leia Organa', 'F', 'Human', 'The Rebel Alliance', 1, True, NULL)
,('Darth Vader (aka: Anakin Skywalker)', 'M', 'Human', 'The Galactic Empire', 1, True, 3)
,('R2-D2', NULL, 'Droid', 'The Rebel Alliance', 1, False, NULL)
,('C-3PO', NULL, 'Droid', 'The Rebel Alliance', 1, False, NULL)
,('Uncle Ownen Lars', 'M', 'Human', NULL, 1, False, 1)
,('Aunt Beru Lars', 'F', 'Human', NULL, 1, False, 1)
,('Obi-Wan Kenobi', 'M', 'Human', 'The Rebel Alliance', 1, True, 1)
,('Han Solo', 'M', 'Human', 'Smuggler', 1, False, 7)
,('Chewbacca', 'M', 'Wookie', 'Smuggler', 1, False, NULL)
,('Greedo', 'M', 'Alien', 'Bounty Hunter', 1, False, 1)
,('Grand Moff Tarkin', 'M', 'Human', 'The Galactic Empire', 1, False, 1)
,('Mon Mothma', 'F', 'Human', 'The Rebel Alliance', 1, False, NULL)
-- Empire
,('Yoda', 'M', 'Alien', 'The Jedi Order', 2, True, 3)
,('Emperor Palpatine', 'M', 'Human', 'The Galactic Empire', 2, True, 3)
,('Boba Fett', 'M', 'Human', 'Bounty Hunter', 2, False, 3)
,('Lando Calrissian', 'M', 'Human', 'Smuggler', 2, False, NULL)
-- RotJ
,('Jabba the Hutt', 'M', 'Hutt', 'Gangster', 3, False, 3)
,('Wicket', 'M', 'Ewok', 'The Rebel Alliance', 3, False, NULL)
,('Admiral Ackbar', 'M', 'Mon Calamari', 'The Rebel Alliance', 3, False, NULL)
-- Ep I
,('Qui-Gon Jinn', 'M', 'Human', 'The Jedi Order', 4, True, 4)
,('Darth Maul', 'M', 'Alien', 'Sith', 4, True, 4)
,('Padme Amidala', 'F', 'Human', NULL, 4, False, 6)
-- Ep II
,('Count Dooku (aka: Darth Tyranus)', 'M', 'Human', 'Sith', 5, True, 6)
,('Mace Windu', 'M', 'Human', 'The Jedi Order', 5, True, 6)
,('Bail Organa', 'M', 'Human', 'Galactic Senate', 5, False, NULL)
,('Jango Fett', 'M', 'Human', NULL, 5, False, 5)
-- Ep III
,('Commander Cody', 'M', 'Clone', 'Clone Army', 6, False, NULL)
,('General Grievous', NULL, 'Droid', 'Droid Army', 6, False, 6)
-- Force Awakens
,('Maz Kanata', 'F', 'Alien', NULL, 7, False, NULL)
,('Captain Phasma', 'F', 'Human', 'The First Order', 7, False, NULL)
,('Poe Dameron', 'M', 'Human', 'The Resistance', 7, False, NULL)
,('BB-8', NULL, 'Droid', 'The Resistance', 7,False, NULL)
,('Finn', 'M', 'Human', 'The Resistance', 7, False, NULL)
,('Rey', 'F', 'Human', 'The Resistance', 7, True, NULL)
,('Kylo Ren (aka: Ben Solo)', 'M', 'Human', 'The First Order', 7, True, NULL)
,('Supreme Leader Snoke', 'M', 'Alien', 'Sith', 7, True, NULL)
-- Rogue One
,('Jyn Erso', 'F', 'Human', 'The Rebel Alliance', 8, False, 8)
,('Orson Krennic', 'M', 'Human', 'The Galactic Empire', 8, False, 8)
,('K-2SO', NULL, 'Droid', 'The Rebel Alliance', 8, False, 8)
,('Saw Gerrera', 'M', 'Human', NULL, 8, False, 8)
-- Extended Universe
,('Grand Admiral Thrawn', 'M', 'Alien', 'The Galactic Empire', NULL, False, NULL)
;
