-- My custom Star Wars character database for testing fun...
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
--     never learned to be a Jedi???"... then: you should write a test of
--     Figurine's UPDATE function to make it the way you want it.
-- 2.) If you find yourself thinking: "But you forgot <character x>!? (S)he is
--     the best! How could you forget <character x>?"... then: you should write
--     a test of Figurine's INSERT function add that character. Unless yousa
--     gonna add _that_ character, in which case turn in your nerd creds.
--     Meesa done here. There's no hope for you. If you're missing Wedge
--     Antilles or Porkins, well done!
-- 3.) If you find some other nit to pick, post your own dataset to a site like
--     https://www.kaggle.com/datasets. Devs like me sure could use free, light,
--     easy to grok test datasets for use in projects like this. And don't
--     forget to license via Creative Commons or another permissive license.
--     My sincerest thanks!

CREATE TABLE movies (
    id INTEGER PRIMARY KEY NOT NULL
    ,name TEXT
    ,episode TEXT
    ,director TEXT
    ,released_year INT
    ,chronology INT
);

INSERT INTO movies
(name, episode, director, released_year, chronology)
VALUES
 ('Star Wars (A New Hope)', 'IV', 'George Lucas', 1977, 5)
,('The Empire Strikes Back', 'V', 'Irvin Kershner', 1980, 6)
,('Return of the Jedi', 'VI', 'George Lucas', 1983, 7)
,('Star Wars: Episode I - The Phantom Menace', 'I', 'George Lucas', 1999, 1)
,('Star Wars: Episode II - Attack of the Clones', 'II', 'George Lucas', 2002, 2)
,('Star Wars: Episode III - Revenge of the Sith', 'III', 'George Lucas', 2005, 3)
,('Star Wars: The Force Awakens', 'VII', 'J.J. Abrams', 2015, 8)
,('Rogue One: A Star Wars Story', 'VIII', 'Gareth Edwards', 2016, 4)
,('Star Wars: The Last Jedi', NULL, 'Rian Johnson', 2017, 9)
;

CREATE TABLE characters (
    id INTEGER PRIMARY KEY NOT NULL
    ,name TEXT NOT NULL
    ,sex TEXT
    ,character_type TEXT
    ,allegiance TEXT
    ,first_appeared_movie_id INT REFERENCES movies(id)
    ,has_force BOOLEAN CHECK (has_force IN (0, 1))
    ,died_in_movie_id INT REFERENCES movies(id)
);

-- 42 of them!
INSERT INTO characters
(name, sex, character_type, allegiance, first_appeared_movie_id,
 has_force, died_in_movie_id)
VALUES
-- Star Wars
 ('Luke Skywalker', 'M', 'Human', 'The Rebel Alliance', 1, 1, NULL)
,('Leia Organa', 'F', 'Human', 'The Rebel Alliance', 1, 1, NULL)
,('Darth Vader (aka: Anakin Skywalker)', 'M', 'Human', 'The Galactic Empire', 1, 1, 3)
,('R2-D2', NULL, 'Droid', 'The Rebel Alliance', 1, 0, NULL)
,('C-3PO', NULL, 'Droid', 'The Rebel Alliance', 1, 0, NULL)
,('Uncle Ownen Lars', 'M', 'Human', NULL, 1, 0, 1)
,('Aunt Beru Lars', 'F', 'Human', NULL, 1, 0, 1)
,('Obi-Wan Kenobi', 'M', 'Human', 'The Rebel Alliance', 1, 1, 1)
,('Han Solo', 'M', 'Human', 'Smuggler', 1, 0, 7)
,('Chewbacca', 'M', 'Wookie', 'Smuggler', 1, 0, NULL)
,('Greedo', 'M', 'Alien', 'Bounty Hunter', 1, 0, 1)
,('Grand Moff Tarkin', 'M', 'Human', 'The Galactic Empire', 1, 0, 1)
,('Mon Mothma', 'F', 'Human', 'The Rebel Alliance', 1, 0, NULL)
-- Empire
,('Yoda', 'M', 'Alien', 'The Jedi Order', 2, 1, 3)
,('Emperor Palpatine', 'M', 'Human', 'The Galactic Empire', 2, 1, 3)
,('Boba Fett', 'M', 'Human', 'Bounty Hunter', 2, 0, 3)
,('Lando Calrissian', 'M', 'Human', 'Smuggler', 2, 0, NULL)
-- RotJ
,('Jabba the Hutt', 'M', 'Hutt', 'Gangster', 3, 0, 3)
,('Wicket', 'M', 'Ewok', 'The Rebel Alliance', 3, 0, NULL)
,('Admiral Ackbar', 'M', 'Mon Calamari', 'The Rebel Alliance', 3, 0, NULL)
-- Ep I
,('Qui-Gon Jinn', 'M', 'Human', 'The Jedi Order', 4, 1, 4)
,('Darth Maul', 'M', 'Alien', 'Sith', 4, 1, 4)
,('Padme Amidala', 'F', 'Human', NULL, 4, 0, 6)
-- Ep II
,('Count Dooku (aka: Darth Tyranus)', 'M', 'Human', 'Sith', 5, 1, 6)
,('Mace Windu', 'M', 'Human', 'The Jedi Order', 5, 1, 6)
,('Bail Organa', 'M', 'Human', 'Galactic Senate', 5, 0, NULL)
,('Jango Fett', 'M', 'Human', NULL, 5, 0, 5)
-- Ep III
,('Commander Cody', 'M', 'Clone', 'Clone Army', 6, 0, NULL)
,('General Grievous', NULL, 'Droid', 'Droid Army', 6, 0, 6)
-- Force Awakens
,('Maz Kanata', 'F', 'Alien', NULL, 7, 0, NULL)
,('Captain Phasma', 'F', 'Human', 'The First Order', 7, 0, NULL)
,('Poe Dameron', 'M', 'Human', 'The Resistance', 7, 0, NULL)
,('BB-8', NULL, 'Droid', 'The Resistance', 7,0, NULL)
,('Finn', 'M', 'Human', 'The Resistance', 7, 0, NULL)
,('Rey', 'F', 'Human', 'The Resistance', 7, 1, NULL)
,('Kylo Ren (aka: Ben Solo)', 'M', 'Human', 'The First Order', 7, 1, NULL)
,('Supreme Leader Snoke', 'M', 'Alien', 'Sith', 7, 1, NULL)
-- Rogue One
,('Jyn Erso', 'F', 'Human', 'The Rebel Alliance', 8, 0, 8)
,('Orson Krennic', 'M', 'Human', 'The Galactic Empire', 8, 0, 8)
,('K-2SO', NULL, 'Droid', 'The Rebel Alliance', 8, 0, 8)
,('Saw Gerrera', 'M', 'Human', NULL, 8, 0, 8)
-- Extended Universe
,('Grand Admiral Thrawn', 'M', 'Alien', 'The Galactic Empire', NULL, 0, NULL)
;
