REGISTER /usr/lib/pig/piggybank.jar;
/* comentaris = LOAD '/user/cloudera/WorkspacePigPractica/resultat_analisis_opinions/part-m-00000' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE')  AS (id:int, text:chararray, label:int, c:int, AVG:float); */
comentaris = LOAD '/user/cloudera/pig_practica/critiquescinematografiques.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE')  AS (text:chararray, label:int, id:int);
pelis = LOAD '/user/cloudera/WorkspacePigPractica/titolpelis.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE')  AS (id:int, nom_pelicula:chararray);
comentaris_group = group comentaris by id;

/* Per cada id contam les labels positives i les negatives */
countOpinions = foreach comentaris_group
  {
      l_positives = FILTER comentaris BY label == 1;
      l_negatives = FILTER comentaris BY label == 0;
      l_total = COUNT(l_positives)-COUNT(l_negatives);
      GENERATE group as id, COUNT(comentaris.id) as n_comentaris, COUNT(l_positives) as l_positives, COUNT(l_negatives) as l_negatives, l_total as l_total;
  }

/* Feim un join de les pelicules amb el seu n_opinions, les labels positives i les labels negatives */
pelis_join1 = join pelis by id, countOpinions by id using 'replicated';
pelis_opinions = foreach pelis_join1 generate pelis::id as id, pelis::nom_pelicula as nom_pelicula, countOpinions::n_comentaris as n_opinions, countOpinions::l_positives as l_positives, countOpinions::l_negatives as l_negatives, countOpinions::l_total as l_total;


STORE pelis_opinions INTO '/user/cloudera/WorkspacePigPractica/resultat_analisis_opinions_pelicules' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE');
