# MovieLens small: frozen teaching copy

`movies.csv` and `ratings.csv` are unmodified files from the September 2018
MovieLens **ml-latest-small** archive: 9,742 movie metadata rows and 100,836
ratings from 610 users. There are 9,724 distinct movies in the ratings file;
some metadata entries have tags but no ratings.

Source: <https://files.grouplens.org/datasets/movielens/ml-latest-small.zip>.
The upstream name contains “latest”; this repository freezes the downloaded
bytes so the practical works offline and stays reproducible.

The original terms, attribution, and column descriptions are preserved in
[`MOVIELENS_README.txt`](MOVIELENS_README.txt). Those terms apply to these data,
including the restriction on commercial use without permission. Redistribution
must retain the same terms. See the [upstream usage license](https://files.grouplens.org/datasets/movielens/ml-latest-small-README.html).

Dataset citation: F. Maxwell Harper and Joseph A. Konstan. 2015. *The MovieLens
Datasets: History and Context*. ACM TiiS 5(4), Article 19.
<https://doi.org/10.1145/2827872>.

SHA-256 checksums:

```text
archive      696d65a3dfceac7c45750ad32df2c259311949efec81f0f144fdfb91ebc9e436
movies.csv   5a5f32dd9bb3797b8e728a1b98958789d2b13f294a69fdfbc5727f8a9611aa07
ratings.csv  aa289ca83157595d0df6aea1be6a4ded676ddc4385472e8313a8ed9805352646
```
