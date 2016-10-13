Question 1 : Bucket Size was chosen to be 10 for each hash function
	     Works for higher values as well

Question 3 : Implemented in Scala
             Using support >= 5 while checking sample input
OUTPUT:
(( 'Affolter, Nathan', 'Affolter, Thomas'),5)
(( 'Acevedo Bernal, Gonzalo', 'Acevedo Bernal, \xc1lvaro'),5)
(( 'Adams, Andrew (VIII)', 'Adams, Ben (XXVII)'),7)
(( 'Abeyratne, Alysia', 'Adshead, Chris'),8)
(( 'Abdullah, Pengiran Hajah Rokiah Pengiran Haji', "Abdullah, Rabi'atul 'Adawiyah"),8)
(( 'Aaltonen, Aino', 'Aaltonen, Veikko'),8)
(( 'Adams, Brooke (I)', 'Adams, Lynne (II)'),8)
(( 'Abbott, Kelsy', 'Abbott, Kelsy'),15)
(( 'Abbott, Deborah', 'Abbott, Deborah'),15)
(( 'Abascal, Mar (I)', 'A. Solla, Ricardo'),107)

Command Used to Run on Spark :
spark-submit --class Vasuprada_Vijayakumar_spark.Frequent_ItemSets --master local[1] /home/vasu/IdeaProjects/Vasuprada_Vijayakumar_spark/out/artifacts/Vasuprada_Vijayakumar_spark_jar/Vasuprada_Vijayakumar_spark.jar "/home/vasu/553_assign2/553_assign2/SPARK/actress" "/home/vasu/553_assign2/553_assign2/SPARK/director" 5



