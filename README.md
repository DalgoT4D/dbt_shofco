Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](https://getdbt.com/community) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

```
dbt_shofco
├─ .DS_Store
├─ .git
│  ├─ COMMIT_EDITMSG
│  ├─ FETCH_HEAD
│  ├─ HEAD
│  ├─ config
│  ├─ description
│  ├─ hooks
│  │  ├─ applypatch-msg.sample
│  │  ├─ commit-msg.sample
│  │  ├─ fsmonitor-watchman.sample
│  │  ├─ post-update.sample
│  │  ├─ pre-applypatch.sample
│  │  ├─ pre-commit.sample
│  │  ├─ pre-merge-commit.sample
│  │  ├─ pre-push.sample
│  │  ├─ pre-rebase.sample
│  │  ├─ pre-receive.sample
│  │  ├─ prepare-commit-msg.sample
│  │  ├─ push-to-checkout.sample
│  │  └─ update.sample
│  ├─ index
│  ├─ info
│  │  └─ exclude
│  ├─ objects
│  │  ├─ 00
│  │  │  ├─ 0f926169161f82fdd5675177f0c2a110568184
│  │  │  ├─ e4e13222fc1f75afdabf5348bd593b7aca4776
│  │  │  └─ e7a492e6bf8aace89edd91150fd56ec120c3c7
│  │  ├─ 01
│  │  │  ├─ 5a930792d972dfd36b1e33bbd098251edb3197
│  │  │  ├─ 866f73d178f6834e3b3957cfce4b276ebd0f06
│  │  │  ├─ ad3dc94e56b48c1dad8a257a4c78d8087ccadc
│  │  │  └─ e202e42cb201dc0052ab53c5aca285e1bf39e1
│  │  ├─ 03
│  │  │  └─ 1e7d280ea735ea2d26b2cf855668ea365ecf2f
│  │  ├─ 04
│  │  │  └─ 19203321d0b1e78a3c9eff576d8fcbcd5de0db
│  │  ├─ 05
│  │  │  ├─ 74692bd6bfbd96fb0e546b3573316e1da533bf
│  │  │  ├─ 8e2c424f317f59113e5e843817b57b1e8b8563
│  │  │  └─ ad416d07c222ad2abaeb493d2d5ef1c574d465
│  │  ├─ 07
│  │  │  ├─ 4ba94b87ad25af2d0eb32dd8c757a040d8138f
│  │  │  └─ ae44e1c5179284a3183bd57995bba950dfd1d3
│  │  ├─ 08
│  │  │  └─ f73de2aa9cd58284d4d27dc140cff066cf709b
│  │  ├─ 0a
│  │  │  ├─ 72014183ada440564da66f3406573ff1c06cb8
│  │  │  └─ c5632e668e192afa157e0a1815499e302198a7
│  │  ├─ 0b
│  │  │  ├─ 41d3409f1f72c6db067e69986bb711fc9b5794
│  │  │  ├─ d8368ee8e05c8039766d2b8b295a1787e4b941
│  │  │  └─ eb7c9f1fcd10c02586b015c6647e5360a24040
│  │  ├─ 0d
│  │  │  ├─ c9b0a6d677d3cfbb88e680ae94436d9ffa5b76
│  │  │  └─ e196370ba60ff177bdd2deff7f1f6efc9358a3
│  │  ├─ 0e
│  │  │  ├─ 045f1fd8e89f23aeca7ad3141fd116277bb6a5
│  │  │  ├─ aba3f68daf23a3c4f91428b24a5b75a195b024
│  │  │  └─ cb615bf91a05b301ac75b79ba156039f5c3261
│  │  ├─ 0f
│  │  │  ├─ 1b77109246d4e2859546103740bb2d0e271970
│  │  │  └─ 5ace8a3ab3d46d710377c422613c67c59d2720
│  │  ├─ 10
│  │  │  ├─ 1ed2f0b51c907c39d9d0d620429bbea17260a1
│  │  │  └─ a2ad95e245fab3ea54d93125a5d9fba3f39f61
│  │  ├─ 11
│  │  │  └─ ed947dc3686429079d66fe311f9f306fca9c21
│  │  ├─ 12
│  │  │  ├─ 197b44cc7149a3b109123eaa478f989b22b80d
│  │  │  └─ 906b6cabcb14ffc08282ba6410f47e5c8fa65d
│  │  ├─ 14
│  │  │  ├─ 3e57f8589d77322f72d90740c4256e08a3008c
│  │  │  ├─ d446c5bfb321a8afb6b858bb1ce04689cf3674
│  │  │  └─ f12192adbd06deb82ec02495c711abca10498b
│  │  ├─ 15
│  │  │  ├─ 8ed962c9bc5112337acb6eba6d4ddcf267649f
│  │  │  └─ b887800c044ddd9fa3458d293c6e9285ef42f2
│  │  ├─ 17
│  │  │  └─ 60b7258360a94589933440053740e842206098
│  │  ├─ 18
│  │  │  ├─ 35aed386d8a2be9728e7ddbe8038fbeff4d90f
│  │  │  ├─ c9e0ebf549b7de0b4d32c7e3a9631d9df7aaa6
│  │  │  └─ fdb14d38a1b21780299aa8aa80bcc08d505e85
│  │  ├─ 19
│  │  │  ├─ 2fd7cd5672d065a8ab882d195bf6646c76fbb1
│  │  │  ├─ 5c376c39da749db0f006b5c977073d218e711b
│  │  │  ├─ 629972547651b912592901a2e737af505e7899
│  │  │  └─ e7492ad6cb2106c69451683c1dba4d31f1c235
│  │  ├─ 1a
│  │  │  ├─ 0d03cf5c7bf8695572e48ff0f81007b78c6676
│  │  │  ├─ 3d3b0bef1f7e31326672b55b5dd1f5dd6a5893
│  │  │  ├─ a78b4ef2ba53e2ea7f3cc9a51de7f9ec863145
│  │  │  └─ cb5a25b43e0cc5ce5c6bdcba8d52f7d176cc3f
│  │  ├─ 1b
│  │  │  └─ 1f232ad16116507d36bafcdb252e415c2c67d8
│  │  ├─ 1c
│  │  │  ├─ 27bbd386bf1417f0531f3d69180a90523fcee4
│  │  │  ├─ 8edcc0570e3dd3e55613a48676e79efcb308fa
│  │  │  └─ ac71a8f7ae20b4c7d7a49c2d5862d2ab589b09
│  │  ├─ 1e
│  │  │  ├─ 0b6c933210b59d7053a51adcf38281de64e1e4
│  │  │  ├─ 8b3776dcd8fd8666d68a7009db74f80b1dc808
│  │  │  └─ 96707cceb8436eb98e4de72bfe001482e06c40
│  │  ├─ 1f
│  │  │  └─ b5e00b9868149c32dbf7333fdf65c0ed9631d1
│  │  ├─ 21
│  │  │  └─ 2d172b98b86154997b6236cb70412210155420
│  │  ├─ 23
│  │  │  ├─ 12568724917acf7a3f17d367b19ed25e86b108
│  │  │  ├─ 386a94cfc7ddc2ea183de47b2db34a6f4927a3
│  │  │  └─ b196c40d9a1555c54c1cdc7bcb63dc43601f6b
│  │  ├─ 24
│  │  │  ├─ 3aa59a20a1792220c1a9229c5737b286b0ca8c
│  │  │  └─ d6309438d1b1fbbbd94c41f5f1da8a3a3ae547
│  │  ├─ 25
│  │  │  └─ 5541f634c184b087cb5d61861dc8b0fd3a5d78
│  │  ├─ 26
│  │  │  ├─ 273711b0de9e7535b106c3ba286e432b9b85f5
│  │  │  ├─ 775955e61ec97f1b0dca7358704ddea02c9ddc
│  │  │  └─ e4c17821750c2c817ab535e5d6fa71cf99156e
│  │  ├─ 27
│  │  │  ├─ 0129a95d858cf6f51ac588e0864144ab449abf
│  │  │  └─ 1666f8fac1f2d1b679159f6e4756926d14068d
│  │  ├─ 28
│  │  │  └─ 95298fee875bc0200510d92727a2457ca2e8f2
│  │  ├─ 29
│  │  │  ├─ 1feb7e20eaa3e7c7d1d56df00b0923e3357627
│  │  │  ├─ 54694d51b532b2966c5d4b194307fa9a83f79b
│  │  │  ├─ 56a30aeac0efc577438b9ecb0b44f898d2d862
│  │  │  └─ ecc202fe69168df0dd6d996b554d508a221404
│  │  ├─ 2a
│  │  │  └─ 54e805150e51e62d6a4aebe9eaafcd23528702
│  │  ├─ 2b
│  │  │  ├─ 5639978f20d2256324966aa9dac97eb8f0146a
│  │  │  └─ 5955280fa3aefd02090bbab6280a878cbac9d9
│  │  ├─ 2c
│  │  │  ├─ 58c5123c95daf9fd8b5451aca4eb3e6584c277
│  │  │  └─ cd816cdb779407d941f6078af7601846a12c7f
│  │  ├─ 2d
│  │  │  ├─ 066ecd262ed78ce27c0dd0cb002c0dc0c1fd39
│  │  │  ├─ c925985b1ab7649b8a4bf56de47a6931e97fb0
│  │  │  └─ f3bd9ae76680422ba08639287ffe14552159d7
│  │  ├─ 2e
│  │  │  └─ fc878c18d9360a385cda9d131f5163d43b0b9c
│  │  ├─ 2f
│  │  │  ├─ 6f24135097eea06ec760f9af0bd0db95e6de86
│  │  │  ├─ bc0b940b57ce5734212412184a2f68b941a59e
│  │  │  └─ cc114cade6c60fdd9034ae3a9e209c763c7d26
│  │  ├─ 30
│  │  │  └─ a7626a287edd634ff72625fa14ed36822a14c3
│  │  ├─ 32
│  │  │  └─ dbc5e50c5383975f8873374f3fc5ebd1c6e35a
│  │  ├─ 33
│  │  │  ├─ 22b1870e11f7660ac8c55346f3eab0f62003d8
│  │  │  └─ 9853dbe114a4ddde2cc5c3503dffd4fc912a9c
│  │  ├─ 34
│  │  │  ├─ 535c782c71e57129e9ac959646396343472e9c
│  │  │  └─ fb381809145cddc1c2b562fd1541e4d34b9a8b
│  │  ├─ 36
│  │  │  ├─ 603552ae17d892f29c4f0f693baff05fe06221
│  │  │  └─ 7002fdc5c639dc10d20a4f066dc0088e0c7460
│  │  ├─ 37
│  │  │  ├─ eb8c8ffcdf3da4f6000ce0b9813b756657ede6
│  │  │  └─ f28016d8ad869ca4fab3822f1daa515e00b1f9
│  │  ├─ 39
│  │  │  ├─ 26e6349d4da4b7964d2dff1e349568957e0bad
│  │  │  ├─ 90d4d37c44dbc67f5913b54941c3ada26926d1
│  │  │  ├─ b871ca00974bb9d834196818d6e8456a6fb194
│  │  │  └─ d3572b9950fd61f7a706bb639aefe36f56ad97
│  │  ├─ 3b
│  │  │  ├─ 625312b8b3fda9627263c8ed886990b9c24c48
│  │  │  └─ d270e5ccc79b22ea1840e9983cb586fb54a04c
│  │  ├─ 3c
│  │  │  └─ 5559bbb46780d66eae63048b73fe171144ebd0
│  │  ├─ 3d
│  │  │  ├─ 11811fcd263a0dd684d1195143441d71dd0299
│  │  │  ├─ 913c87df586585ee071da8caa61a632f405cd4
│  │  │  ├─ bba301c835df89627c6ff6eafe1932bd5ffc01
│  │  │  └─ f42c8ee39798764071be2458db14294aca2578
│  │  ├─ 3e
│  │  │  ├─ 130a01dac47ab9f9adf63a729f60a1f15fa154
│  │  │  ├─ 81e838426dbc4b890521b515c955a9a4a72eb2
│  │  │  └─ b5c019ff4e805acc07fa2bb458d495e218c86f
│  │  ├─ 3f
│  │  │  ├─ 1f735088e8e55ee74834ace6f39600376d11f7
│  │  │  ├─ 50d48ef316f3a7712f5b3f0a9bcfe91ac05cf4
│  │  │  ├─ 8784373ff5c08fcf43db5eaf7debb843df1a57
│  │  │  ├─ 88987bf7e8d9bc60e3afa5cb90ffd8430615c7
│  │  │  └─ ef050becfd32689739a39f4e8d886fbd9b1086
│  │  ├─ 40
│  │  │  ├─ 573c5a26fd092ac6595031c73890ee1d78fcb9
│  │  │  └─ a5991898bd84423f949cec7efc12646d8597db
│  │  ├─ 41
│  │  │  └─ e33c6c31e68a8f398252f560ac0aa8d572673a
│  │  ├─ 42
│  │  │  ├─ 69c88658d1ef6b563b1bbbd9f938ad5e62825b
│  │  │  ├─ dd98e45ef32a94c0bd45677b4e7ced90b55d2c
│  │  │  └─ eceb024167faf16743e4efd4b0a3e286b5f2b5
│  │  ├─ 43
│  │  │  └─ 338b61ec09711c4e948b0ea008fcd0febc30ec
│  │  ├─ 44
│  │  │  ├─ 0815b9b5ff758379e89a46a29494bfb05df92b
│  │  │  └─ 3ad974e2ef19a889ae03b762a2c2b132766a55
│  │  ├─ 45
│  │  │  ├─ 299fa660569286948855c8156e622dd870746d
│  │  │  ├─ 680d0ae840fd66298456437eef03534892b09f
│  │  │  └─ ddeb6228d75ca412be8640b424271c44e40aef
│  │  ├─ 46
│  │  │  ├─ 0429231ea80fb65de0ed2979810905ae9456cf
│  │  │  └─ b93873382bcbbe6ce3b825851a92b983ca2a03
│  │  ├─ 47
│  │  │  ├─ 0b9f5b2ee90539509208bd1f13a0cdc4bfd170
│  │  │  ├─ 88102b06639987032075aea4accb5cf4851f3c
│  │  │  └─ 9cc6ee9d77890ec73b2c86235765e38c7b9348
│  │  ├─ 48
│  │  │  └─ 751e247647ed9816b00b5ee67928b6fab0a868
│  │  ├─ 49
│  │  │  └─ 6f496d2e496317ee4a22e7780cf608ea2e6099
│  │  ├─ 4a
│  │  │  └─ f9f54a7824d911c54706aba20b2afaf265db75
│  │  ├─ 4b
│  │  │  └─ a842e297b4fd360796c535a5d166eddcbdf7c3
│  │  ├─ 4c
│  │  │  ├─ 27550369a3e4adaabfc83a93a05a5c99fb3061
│  │  │  ├─ 4548d29a82cd44ff1ae0bfc2c8f6c8cd384693
│  │  │  ├─ 5a59a6a03e4eae6b003324c5828de8087fd5a2
│  │  │  ├─ 5e0f2505c422d47fd4a258a986e3ef23b90250
│  │  │  ├─ 82d9b7711b40642057711e8bc06bd2873df9b3
│  │  │  ├─ b8992ea8bdd447a9c9c18c66f7d2d7b16390a9
│  │  │  └─ ce837ef2f98a892b5a2712b1a2290635fb77df
│  │  ├─ 4d
│  │  │  ├─ 3132c5039d86c1e7da3da6ae964562b2d6dbe4
│  │  │  ├─ 7771e4a2a6b54b358dacbaf8df9b5b9bc09142
│  │  │  └─ 91548f4cde9d2afbf1a6ace0ee0c7659ea70b3
│  │  ├─ 4f
│  │  │  ├─ 42474809724ea7af8c5c6247c3207e6901f5a1
│  │  │  ├─ 7d0a8f74dfde40d7cf42683dfe75746f59484e
│  │  │  └─ 8cf5b9b1acca10aab25f53a7e94db167afa228
│  │  ├─ 50
│  │  │  ├─ 0bb66b242ff595ecf5a30066c62d652e51085a
│  │  │  ├─ 5f5c8e49bc2900a08e737dcd74adb2933ac354
│  │  │  └─ e9215454865a203321ee1e6cc58c60448edb50
│  │  ├─ 51
│  │  │  ├─ a1f6e86cae662009774d77d31103051e0a5a03
│  │  │  └─ d60f6b44debfd104c09d004af58c0c929620bc
│  │  ├─ 52
│  │  │  └─ 1d2728c5200687cb3564faaff9950f84328db7
│  │  ├─ 53
│  │  │  └─ c0155304733e6e5a312e616fec256257a0116f
│  │  ├─ 55
│  │  │  ├─ 0d9a6cea2c909e170dc15418b16c21f057b1bb
│  │  │  └─ dfb05cb72971d935b712c30c97c5484aba12a1
│  │  ├─ 56
│  │  │  ├─ 0545011366b95441af3b50fe542a166a454cf7
│  │  │  ├─ 69d79f0d5f550add78abcd2b6bef771d3a772b
│  │  │  ├─ 88e864b9ff577d0bf0136e7db73c8f62d4c378
│  │  │  └─ 8a7f6b90f436643ff9d78921ef9624881b9b3e
│  │  ├─ 57
│  │  │  ├─ 3d30b16234948b47dc9a799e36e8cbe7675300
│  │  │  └─ da5454d894aa5a6b0f835cc447295ad515a2a0
│  │  ├─ 58
│  │  │  └─ 2ae4e6e0ecd6f5aad9659818db537337259604
│  │  ├─ 59
│  │  │  ├─ 0a6e742d87dcd19e3cfc0fd077117cb4535ecc
│  │  │  ├─ 35baa5f33d562161d07f7b205d8955dae84f2d
│  │  │  ├─ aa365ade53db0f696ab33e9dcbe2bfc7137aa7
│  │  │  └─ bee3dd9d7063acb0063be6e22aa8d6ca5681ae
│  │  ├─ 5b
│  │  │  ├─ 324a3aacb137e66988cc034a8739e420137738
│  │  │  └─ 6b6f142f1400e4c3c28b86691b2896c71c8f70
│  │  ├─ 5c
│  │  │  ├─ 504e42471efae4c601cab9fbc23004a6949ca8
│  │  │  ├─ 522f79deec94d8a9e28fc66f97ed6eb0b26b37
│  │  │  ├─ 9e485b8fee67131b3f59c300a52aa9e6bd4e8e
│  │  │  └─ aca9146c7f62c98b53cc8239a70592192bf1a5
│  │  ├─ 5d
│  │  │  ├─ 9395f3bf957e066caa62a00a0b30c9320aae89
│  │  │  ├─ a9ca5fd8d36fbe1ef8a2e32df80298acf3e575
│  │  │  └─ d6f3ed7ac2bcb30664c894e99f64ccbe47fe40
│  │  ├─ 5e
│  │  │  ├─ 05b76f65bb51dd62b844e895d30d83391a4526
│  │  │  ├─ 29c306b0b5dc7f67f79cbca8bffe0e9a25760a
│  │  │  ├─ 47c53b6ea6a075a446fe813ffe6f44c78d7690
│  │  │  ├─ c9896f64cb2c5a9f192ae5bdb3196deac31880
│  │  │  └─ fd62dd34c59256e8aa4e7da13b7feab5808257
│  │  ├─ 5f
│  │  │  └─ 59bc7a96a279b9239ed635f7c486f8187d8c99
│  │  ├─ 60
│  │  │  ├─ 5ea7ff7e058806d16f55e911aac213ff239912
│  │  │  └─ 7635bc50b031a32b3bf616dbb2c1a3f48b8572
│  │  ├─ 61
│  │  │  └─ a58739a8638857f1ab9b746b1e7bf01c3395f7
│  │  ├─ 63
│  │  │  ├─ 87ecd6eb535587e833388b15e7a86649dde128
│  │  │  ├─ c56f69a5348e3312b14dd3dcbf2d39ff3867b8
│  │  │  └─ d74fa147effa2f3d7446e7cb5583c7ea98676e
│  │  ├─ 64
│  │  │  ├─ 94eb3bef133b77227c9d16db936260b4625a14
│  │  │  ├─ d194a1a5ef81fce275bb7243368d75a3f501a5
│  │  │  └─ f4447c29a0fb5130c1dbde4985278dc2b42f75
│  │  ├─ 66
│  │  │  ├─ 172d532ba8a923c497dadb0a7995a499b2a1e7
│  │  │  ├─ e04aa074fac06fbe0ad35637195cb39fa7d923
│  │  │  └─ e8409762482a64e6a4fcfd1c7d69c0c3cde825
│  │  ├─ 67
│  │  │  ├─ 24d131e0fafa62efa31997345a0001903938dc
│  │  │  ├─ 64c3483e308095b4d2654ec3901a3fa079c47f
│  │  │  ├─ 70414f7a3e68d558df8166dd9dca2c21be32f7
│  │  │  └─ ffcb140fad641b245e614d90ad3961c9aefb0c
│  │  ├─ 68
│  │  │  ├─ 140248c14590f7bca10dc79aaa691fc73633a9
│  │  │  ├─ 50c3fe976504e58b8b2713f088ffc05555061d
│  │  │  └─ 8626bbbc57e78f8d1a1befa9c20938d94f24c9
│  │  ├─ 69
│  │  │  ├─ 4c37e45a91c809a1f5c76fe7724b65611848af
│  │  │  ├─ 7201220b6f4d31c2b0ca81c8f65b0a9ff584ad
│  │  │  ├─ af14768b0518a02e78d3c5f66429977cdd7ea6
│  │  │  └─ ed311ef113662487ab962b4e93f25f1a27b545
│  │  ├─ 6a
│  │  │  └─ 8af2711c3234a2e6ca841eb29ef745d4c138b4
│  │  ├─ 6b
│  │  │  ├─ 50feb62c4950f1d4d6398b459e28088e52d464
│  │  │  └─ f645ccb39ebb6699b16d66f55de9ab00cea95a
│  │  ├─ 6d
│  │  │  ├─ 25602f12e224bac75fa570d98a1462e3e57ee5
│  │  │  ├─ 6d12d6e7d32a4fb33e8a4ab71a215a99e56485
│  │  │  ├─ 8c412ba03e3506517781083aef5ba9ec5296eb
│  │  │  └─ d06da0999f455ce4510a55086227d692bff3ba
│  │  ├─ 6e
│  │  │  ├─ d9c808cbcfa8510b350c6f8e760053b7a87f96
│  │  │  └─ f3bb0c340c0677706ae1a3cea9e2136bb6258d
│  │  ├─ 6f
│  │  │  └─ 01c7cf33add5e6b93347fdafa85c8660283f9a
│  │  ├─ 70
│  │  │  └─ c017d8aa1ee6808c63e5233ab5b345b9974d34
│  │  ├─ 71
│  │  │  ├─ 0e14a795a1fd85fdec2e470e58c876e9e977af
│  │  │  ├─ 137e0eff6ae5aefadf8b20def0af1ab036418f
│  │  │  └─ 9c9f62d25f5ada076f3839c4d1b1700056a3cf
│  │  ├─ 72
│  │  │  └─ c5e182aa360f0ee555b387fa75d293c79eb321
│  │  ├─ 73
│  │  │  └─ e56024f3f95193cf02ae46cbb6ce83a47ef574
│  │  ├─ 74
│  │  │  ├─ 25cdd37baa3612d6a1c7cc9cd7e97ebc876c76
│  │  │  ├─ 3079c89ba20904f8f56ca05bc314c6bb8a1306
│  │  │  └─ 574044948bc466680fbb0319c1a6f830c5ec6b
│  │  ├─ 75
│  │  │  ├─ 2495b3d7ec56c810a14da23cf5e7cab80df32e
│  │  │  └─ 28f66bde2d266351697caf4fd23711532e15b1
│  │  ├─ 76
│  │  │  └─ d7df58f53c5f075cbfdbcc50594b8cf6a00947
│  │  ├─ 77
│  │  │  └─ 6743bb5b1121d5d28bd477e5ce2dce6c796845
│  │  ├─ 78
│  │  │  └─ 5757b848fc8295e8decd614d02efa70fd78931
│  │  ├─ 79
│  │  │  ├─ 63ba56f6e8db5444f9c3a0cfe77b1824c198cc
│  │  │  └─ fa6731bcec81ff0f5a761f20f181da7419643a
│  │  ├─ 7a
│  │  │  └─ 729deaf98723ae677e9047c92278629b2345c8
│  │  ├─ 7b
│  │  │  └─ 17ed9a38e1a8fe346021073f074a60b93eb5ad
│  │  ├─ 7c
│  │  │  ├─ 744b3066c068f0fb4f52604e8eb00e92025c42
│  │  │  └─ e9871602fcdc214e1df41aa184443153db759c
│  │  ├─ 7e
│  │  │  ├─ d815de43501ed327c52f51b81cbae9aa19a89b
│  │  │  └─ f9cf46de07e756a66db6dd317109e38f914517
│  │  ├─ 80
│  │  │  ├─ 63dbb4cecc3f778263100eb82a70e5059491d7
│  │  │  └─ c12148dddef7e4d702830daf6c6b4f9f7d7e31
│  │  ├─ 81
│  │  │  ├─ 42cf24eb92185e78863a1d6fdf90c1f8101e1f
│  │  │  ├─ 5eee38321ac32dd1ba55d0ad9587da668931e4
│  │  │  └─ fc15b1b71dc4a9a7d5e48a010b22a890425926
│  │  ├─ 82
│  │  │  ├─ 86ab942bd487af4e28b15068615c45c88bba8e
│  │  │  └─ 9c4f749837b23847d8e0645101e0cc3643a69d
│  │  ├─ 83
│  │  │  ├─ 5ccf3b389c6e5a06a11b81d6a4b608d493a71b
│  │  │  └─ 9d270b0df8647c789d176c7e7a2f7a9568f27e
│  │  ├─ 84
│  │  │  └─ 5f721a9d6ae0934a8b05e3d3d7081cbf5646ec
│  │  ├─ 85
│  │  │  ├─ 0b12375cea5ac8b841b28b2cefd88045f38abe
│  │  │  └─ 2669d78fdec5eb7497f63f52d65cd5a1dc78b4
│  │  ├─ 87
│  │  │  └─ b22148be1830df17efe30e7a02bd55144d67ab
│  │  ├─ 88
│  │  │  └─ 08221d3bb23675feac89117162a98555aea322
│  │  ├─ 89
│  │  │  └─ 9825c6ecabfe30b3e5b60e366026344da83c72
│  │  ├─ 8a
│  │  │  └─ ba71503288a824ee46b5c32a3c69ad493c5212
│  │  ├─ 8b
│  │  │  ├─ 224de1bf05a3fa0980cc1be6ac01d5212e4d51
│  │  │  ├─ 8ee98b1e892516e4e414ba4f44297099bd24e6
│  │  │  ├─ cfc89f2c9ab3730c4520223dcece72312120d8
│  │  │  └─ f2b1ccb67098dee9d2ee1f0c442d894a53e163
│  │  ├─ 8c
│  │  │  ├─ 7c485880b7dffab7935511b2cd0fb65f4c6850
│  │  │  ├─ ddb65174a94a381bb0cf7a23140ddce8f7e247
│  │  │  └─ f7b939849c5a0d2f5b8a6c8bd429ad2ab84d5a
│  │  ├─ 8d
│  │  │  └─ 768bd87ec7d3038cf11617807b12041976a5cc
│  │  ├─ 8e
│  │  │  └─ 76e7df3fb230e542c5e56a865642478fbc0544
│  │  ├─ 8f
│  │  │  ├─ 207b3ad457a974c3fd3117b08028323e99ab5e
│  │  │  ├─ 2fe0ea2449aeec1652adc0a4bc1cf4aa6ab89d
│  │  │  └─ c6ebac2980ac2b8919abba553897f998f1e958
│  │  ├─ 90
│  │  │  └─ e37e34415bf179efb2c07c2ab4ad84941b6743
│  │  ├─ 91
│  │  │  └─ 4b8686b2409463f445fa52c0735abdba0debf2
│  │  ├─ 93
│  │  │  └─ 3abf306c7df4be225c693c086dae5a5fe8a8c8
│  │  ├─ 94
│  │  │  ├─ 517da1d2993a6c1eecb831dd504541474b5519
│  │  │  ├─ 7f0612280734bb124e95280ae8f8c31cd1fbe5
│  │  │  ├─ 9c35cdc58654357623a143a38ddc9e9dfc6cbf
│  │  │  └─ bd7f19a6c10dbb0d34d2aafbfa66c03d948a9b
│  │  ├─ 95
│  │  │  └─ af1d4b583629dfbe7376365e6a40685e260b41
│  │  ├─ 96
│  │  │  └─ 4260a45838d0c7ac0b5b12420b5ddd11093eea
│  │  ├─ 97
│  │  │  └─ 5f9b1161316467d153b56f648c6e2ee858f402
│  │  ├─ 98
│  │  │  ├─ 205cae1b7cea26f2899ac3b41e20f7456318fd
│  │  │  ├─ 339347924e230b56353db3b557ed8da0fe7099
│  │  │  ├─ 8d1ac2a544e0297838972d2b1d2b419578572e
│  │  │  └─ d8beb7006a564ab8d6773bc24d3072f81d5774
│  │  ├─ 99
│  │  │  ├─ 1ba16d0f731ac50ce37165f3a23aff487d711e
│  │  │  ├─ 96bccb4a22ae4e7ed18884c07ac0d417569bbb
│  │  │  ├─ a2799eb1aba56350ff73242cad1bac7f041a90
│  │  │  └─ cc92ffe5b5d1db6cf35499b4c69f4dd7f94b78
│  │  ├─ 9a
│  │  │  ├─ 5f76fb4e509a6fcdb7d250daf509c385a8d87e
│  │  │  └─ 887d9511f3a01f9e79529e9d9ad4cde8f18035
│  │  ├─ 9b
│  │  │  ├─ 09e6284ae5f86a84374104496685491a9bbc9c
│  │  │  ├─ 16fbb9bec8e4bccf1c58beccdbbd2a29fc53c6
│  │  │  └─ 1c8cd44cdf5228e410cf353edef945f5ef69ba
│  │  ├─ 9c
│  │  │  └─ efd74a89299894ae735ded1835839f82cf012a
│  │  ├─ 9d
│  │  │  └─ ee04b7c26b6b34a2b0aba897de28e60bea7189
│  │  ├─ 9e
│  │  │  ├─ 3e0078fd06516435f029b2a1da6595a1454524
│  │  │  ├─ 960f3e76440d0de65d49f0f193fad5e34813cd
│  │  │  ├─ 9f576cd84aa87e3465bd5353dbd5c75c06e3dc
│  │  │  ├─ a2b30abf7d01ba628fc6fb4de397e2d3037162
│  │  │  └─ e0d0e493c1ad380d30d7d4fca205f21a9e063a
│  │  ├─ 9f
│  │  │  ├─ 394e5149c929282be15fd61e7d2a652a0b5be8
│  │  │  └─ ad7aabdc40d79263a9e7483cdecf12385aa31c
│  │  ├─ a0
│  │  │  ├─ 1c30ec2ed1d0526f9bce1a9689b75887289cfb
│  │  │  ├─ 514b4eec340f3f275fa22fba411f2ea07c3a87
│  │  │  ├─ 91e79eb8d47fdd920139117e4298b674e2ea6f
│  │  │  └─ 95b9596db6516e17569a3cc140baf7602c984c
│  │  ├─ a2
│  │  │  ├─ 5ff2606e7b373817999b0bff1818c3d2ac8efa
│  │  │  ├─ d5aa7877affa56fec50ef7ad19d7dade42cd94
│  │  │  └─ e10bde3bc0520afc508ba37000f6b79fd83361
│  │  ├─ a4
│  │  │  ├─ 0c2c25126063f1662615d2d45587163d99ce0b
│  │  │  ├─ 6b5b3a5f194e0a2d761034eb0e0a2113052bbf
│  │  │  └─ 7d3cc3ff25e14d40377a768fb774975ade3eca
│  │  ├─ a5
│  │  │  ├─ 3d5d8b421f58824cd4b95419ef6853b417e7be
│  │  │  └─ cbf477ec0ed7f6acbae1dd044d2cf38c6656a9
│  │  ├─ a6
│  │  │  ├─ 079ea7b32999001b9d7646c4693050e8b35d8c
│  │  │  └─ 31760cc20daefb5442f6b75b2c788b8205cfc4
│  │  ├─ a7
│  │  │  ├─ 8d8e1cf48f9d78ab9190bb353a09373eff2c43
│  │  │  ├─ 99f2c3bcf45b59fdbdc54090f2a174041a4f28
│  │  │  └─ c9d78ad79ee7b5eae7f11cd2499812909d8a6b
│  │  ├─ a8
│  │  │  ├─ 0cecd9518354d0b8f10f6b43aa39321975d740
│  │  │  ├─ 11b902b4590c1feb8b5543f2136aee4ebe6564
│  │  │  ├─ a673e473dd24d8355e4efa3eeec87b721cee02
│  │  │  └─ faa452146e10ba29f2d404878d39964cd69b8a
│  │  ├─ a9
│  │  │  └─ 875a9e9d88fca4d69185f72235c7edf3c0c6b4
│  │  ├─ aa
│  │  │  └─ 179ff5f6a467b5e238375d74bf878ddea6e5bb
│  │  ├─ ab
│  │  │  ├─ b28537af3ce20617c68902042cb79199560c0a
│  │  │  └─ fe356c1032b33851e87cd0d8309c722425f7c5
│  │  ├─ ac
│  │  │  ├─ 0ace7de4614c017ceb4c8d08c36609b7305c0a
│  │  │  ├─ b3e89ba6fc89894792c343495da98ab0fa8c5f
│  │  │  ├─ c258ea13d465e9387d1fd1595c1d2265a404ee
│  │  │  ├─ de2f52be952048eac4fcb7177d6593c1d69249
│  │  │  └─ e2c5b314e54366798ea67e1820c83b5f3972a9
│  │  ├─ ad
│  │  │  ├─ 45488c898a7e6278577e672c3b628e4bd7a6b5
│  │  │  ├─ b27afb8f08145781688518e1fddf1906fca44b
│  │  │  └─ e0cfca542c820fe0d723fe4f8399c319ed7f90
│  │  ├─ ae
│  │  │  ├─ 38d9c22cc79a4ba7cd08a8438f586c15066bdd
│  │  │  └─ 7a5f485473ae83f8d81e92c8a3cb0a99171a06
│  │  ├─ af
│  │  │  └─ 842dd93845b2fd345359b4a0d4a98546865aef
│  │  ├─ b0
│  │  │  └─ c25af8bd9d1e44bab60058ad2584fd32565946
│  │  ├─ b1
│  │  │  └─ bc7ee63b55072248c0ed1a8a630737927f700e
│  │  ├─ b2
│  │  │  ├─ 32616dcf1598ffe7815df94d4200a7150a275f
│  │  │  └─ a496ce95627dc7580dbe3f92a73cf42d3c9dc0
│  │  ├─ b3
│  │  │  ├─ 0ea07a65d13638daf81c785a136d88d5ebc2be
│  │  │  └─ c3c6d72ad831681d9ce9dbc3a1a14bb2de54da
│  │  ├─ b4
│  │  │  ├─ 63df2a337691fdabb27bbbcfd628111e65446e
│  │  │  └─ b32943dc00eb6da31dee5af8018c045f63d5ef
│  │  ├─ b5
│  │  │  ├─ 24b44b065030138a5cc711803bc4937a315c6b
│  │  │  └─ b38ac77c616c2115c64f0cadc4678075a2dfc0
│  │  ├─ b6
│  │  │  ├─ 087924a088f626e3e7ce6b68e1c6fda927ad71
│  │  │  └─ 471895ab69ac726799edb4decd6ac062ec2304
│  │  ├─ b7
│  │  │  ├─ 672e264e2ec2d6fc322ee79647f73820ab0be9
│  │  │  └─ b9df609685a47ac550a62f93932ba185b4e017
│  │  ├─ b8
│  │  │  ├─ 4c3d34186e8420a9ec2a441e5daa3ba51e57c8
│  │  │  └─ ef8f30c0f91056012184bf5b1b5d4a9ab1ae44
│  │  ├─ b9
│  │  │  ├─ 2ecda8138f2e3b0943a5ea00a8b2b580b8be6b
│  │  │  ├─ 54823d6621fb155ee9b08fcb7b0cf267d1e674
│  │  │  ├─ 5f077ccf08642d8170405cbefd14f1b9760751
│  │  │  └─ a62d2b7dff5340cbc6b629672b665f05a61468
│  │  ├─ ba
│  │  │  ├─ 5201e98b0c6f005109c4f24f9c629923cdd6a8
│  │  │  ├─ 85960b5c20558f1059ab858febe5c271f98f88
│  │  │  ├─ a6061c975b67f03e51a60626150f4a48d725b9
│  │  │  └─ c6c6f99357e51e2fc6a0782d53e678be926d79
│  │  ├─ bb
│  │  │  └─ 091512dc5aead4c415fbdbe1a6cf795e6b1a73
│  │  ├─ bc
│  │  │  ├─ 88d7de4e5b16593e493f22fb4c3fa6d4376f26
│  │  │  └─ db2b3d8d0b5e2cb4874930a423adb039f31232
│  │  ├─ bd
│  │  │  ├─ 41da468abaadca85a6a2f250e6912a01ab210b
│  │  │  └─ d51312dfcbf879c5e8b8efbd4240b0ab874325
│  │  ├─ be
│  │  │  └─ 485737f8d37ee76dc6d4f877ec266e219ffac2
│  │  ├─ bf
│  │  │  ├─ 664338c8ebc0a8742ead7d40ca145fcca8b609
│  │  │  └─ 8e07b18b86624b4919b4f0082c00c3cc3e918f
│  │  ├─ c0
│  │  │  ├─ e662aa11c4e0f89a2b75956f9e58d78773d1fb
│  │  │  └─ fa32a895e8b3b0a4e85ca4fc871cffeb79ba04
│  │  ├─ c1
│  │  │  ├─ 3876d1697afe2cde4b6dbc5a6d8c73848cb075
│  │  │  ├─ 3ae3cc8779ea9f2b09bece1d3facf31eccebde
│  │  │  └─ d37eb1578ac752d844f1b7d99d87110e070cec
│  │  ├─ c3
│  │  │  ├─ 2420cec865978961f660ecb53e56f4303a8b9a
│  │  │  └─ 4209d9fb9c3af827ff8c2f73ce236bbf20866a
│  │  ├─ c4
│  │  │  ├─ 9d0c52b8996ecca9ee088c18b937f0eb0b465d
│  │  │  └─ ff954e159533ab7622031b53802cc6f12c1762
│  │  ├─ c5
│  │  │  ├─ 650999b08cb033e856341c87d2f625b9015ea6
│  │  │  └─ cc351b1e1b653788fac49b9cb6cce411879d87
│  │  ├─ c8
│  │  │  └─ 4672f478af9fae84256277142b267ad0628bc8
│  │  ├─ c9
│  │  │  └─ 073adc5719ee9f750e7e922b0a80bff27bb6db
│  │  ├─ ca
│  │  │  ├─ 4604704ae93b2f00ec6e0e30f7f23ac97cde48
│  │  │  └─ da823865b7d2a3bf70c13a387cdf5098566d69
│  │  ├─ cb
│  │  │  └─ 259057f9c40982c8f348200d47ae30b54c95b1
│  │  ├─ cc
│  │  │  └─ 8685d876210176cb8660ece237ac7ddaea30ed
│  │  ├─ cd
│  │  │  ├─ 07d0aabe054ef492dc5696581dc06279129187
│  │  │  ├─ 0a212422646a5c87fd9c1c4ccb65febd2f32d4
│  │  │  └─ a0b82b84917e6ee8e2f21861c711fed5c5666b
│  │  ├─ ce
│  │  │  ├─ 5c57add753e39ded9144aaa8238099c2a026b9
│  │  │  └─ b1238ec9be74301e3648a4404a218455ee6c00
│  │  ├─ cf
│  │  │  └─ 81d8d2229040f94e570ea40082f0b4afbdedff
│  │  ├─ d0
│  │  │  └─ 3cb523d32f9d605886a347624c374056ee56ff
│  │  ├─ d1
│  │  │  └─ eff4224343f8a82faeff77e331aa101dc7b879
│  │  ├─ d2
│  │  │  └─ d984e7eb3da176ba327ef956bfd4b1b5df0eae
│  │  ├─ d3
│  │  │  └─ a8536ce64cfe5347535e09bd7f48c8fceca11f
│  │  ├─ d4
│  │  │  ├─ b0d6edbbd96c8185afdb495e39256c5dd997d2
│  │  │  └─ f1ddb207358db48c498aa71638347937bcc996
│  │  ├─ d5
│  │  │  ├─ 91e06f462e12a5535c676f46e2a1cedb13999e
│  │  │  └─ aa880b96e785c8c28447782f3dd543bcd11c9d
│  │  ├─ d6
│  │  │  └─ d38086a7fdac7c65049157638759b2eb11c3f6
│  │  ├─ d7
│  │  │  └─ 072efb0d6b9e95bf5456f059433c3b47d31f93
│  │  ├─ d8
│  │  │  └─ e05aef9a82ff793a58ad7b59433b34d6f7fb52
│  │  ├─ d9
│  │  │  └─ 72468d2299f67e63432a1f78a824265f3b4818
│  │  ├─ da
│  │  │  ├─ 74229138c99e70493efc4611bf74c49e673e19
│  │  │  └─ dc7b4f6b0ae22c88af5a6e293cd58573a6be8d
│  │  ├─ dc
│  │  │  └─ 296cc87330f6d4919a77d0c8d7e70b7d9bed0b
│  │  ├─ dd
│  │  │  ├─ 39e09ea18825da9b4148a60a15fe90f5a51400
│  │  │  └─ ede036d0e218e39a7f07186b1c54260c139b51
│  │  ├─ de
│  │  │  ├─ 3f7ede7ecd483b5acab6145714165f5b70b1c6
│  │  │  ├─ 805be124bc564df6ff9f0024cbdf55e8140d69
│  │  │  └─ d2d5743f390b36d4d6343c12a7a8d5a9788d88
│  │  ├─ df
│  │  │  ├─ 8f5d3ef6f7f5252ff02c7958ff16e10f436b2f
│  │  │  └─ c7ceb385cce57487ca1f917049d417570b058b
│  │  ├─ e0
│  │  │  ├─ 30c3aaa8b86659de7fce974b9692c1f9d78f88
│  │  │  └─ df80e06a5520a073184f8b90aec03fe50b86ac
│  │  ├─ e2
│  │  │  ├─ 29e88dab518522e8c28f01a762a78858925154
│  │  │  ├─ 998e14bec5a9c40ea63b95b863417ec23a6113
│  │  │  ├─ 9a187e90f639e855debae9c9521f263099c9a5
│  │  │  └─ e3b9e39e9a6a5273fc8cd24b844b7fb8ddd199
│  │  ├─ e3
│  │  │  ├─ 1ebe7fb4550b01f4eb5c75792014db87e1c2d0
│  │  │  └─ 43319a411624594f57439aa00f8db070ccdec9
│  │  ├─ e4
│  │  │  ├─ 87fd4ff98e40125cccaafce4e791141594c2a6
│  │  │  ├─ 89d32993d308b7daba268cc117ee357ec94965
│  │  │  └─ d9faefac78f7d02c71a1f5b58f0d4b89c28b18
│  │  ├─ e5
│  │  │  ├─ 5b8bda43077055dfe8262f15beaccebef51de5
│  │  │  └─ eae5b4c890398675006eed53f0110a3b97beae
│  │  ├─ e7
│  │  │  ├─ 1b58393e95ad90eaca53e4c694dd31457723ab
│  │  │  └─ 3df931fc863cf6772cff213e23efd20bef1a54
│  │  ├─ e8
│  │  │  └─ 84eda77f8c61f5722081eddcf763742e07dff7
│  │  ├─ e9
│  │  │  └─ 8ebd67b7b3199e82029469ab7677d21f9d4ccf
│  │  ├─ ea
│  │  │  ├─ 742826a7db8b6d8e9ac120fc280a478c6c513e
│  │  │  ├─ b7bc64121ff46e4cb2a6ff3ae4a841a95e96b0
│  │  │  └─ e0223f95022856994a95933556263282ec604f
│  │  ├─ eb
│  │  │  ├─ c0c77fc8c6362d4c4de65adebaf5120ee28290
│  │  │  └─ c5762210395053fecfdb8bdb2c71a3f41a23c2
│  │  ├─ ec
│  │  │  ├─ 079c679f25eba136413b0dbb4d0ca01b23a3ca
│  │  │  ├─ 936d435dddb0b1545d09e75f32eb2e42df8127
│  │  │  ├─ 9fadb9e075f2c11ccdafbf986d00e669a93861
│  │  │  ├─ e0b58299126de0472f79f5cd8570ecd7b01d6e
│  │  │  ├─ e7f92f50e95c4fab3a25fd99761b1490ef9739
│  │  │  ├─ ea0eaa77e643e0f5949252888c1c624b486bae
│  │  │  └─ ef8d4945c62e906711c4b6486f8952f6423f72
│  │  ├─ ed
│  │  │  ├─ 7d4bcd192bb2026422412309f2fea4fc6b3df9
│  │  │  └─ b3923c2843f815c27b5133ee938ba140d381a1
│  │  ├─ ee
│  │  │  ├─ 5f665c59a52c0d10587c8110116ae79b0b8791
│  │  │  └─ 77beb5d8385e0ea976a42a6a0181ca0cc8d671
│  │  ├─ ef
│  │  │  ├─ 4c8ba6ba2e00c6366c95c0d44cf0f69208be8d
│  │  │  └─ e6bb947cb3b93f80a1b9336b4f672f34efe09f
│  │  ├─ f1
│  │  │  ├─ 0a0ae13796d6923b24eff58f17f83f91505c02
│  │  │  └─ 1ae6fe101cb5b2ac16be58cd22e60f92a21d04
│  │  ├─ f3
│  │  │  ├─ 31935ec0503644a3f84a656f828578cc5f5269
│  │  │  ├─ 7517c4d5c057e4afd04ed4eecee8f4626b405f
│  │  │  └─ f663cca636e86608701373bf6276cfcc40a3d5
│  │  ├─ f4
│  │  │  ├─ 1faf4ccc2069320fac9c5ace085a472dfd5cad
│  │  │  ├─ 2f5c665e35c9bd8a99ad7a45e047f150e9b09d
│  │  │  ├─ 6379770073628611beb571eea42ea2021f9232
│  │  │  └─ 8967b4f3157b97f0c4b195d1fe83e1a2110780
│  │  ├─ f5
│  │  │  └─ 97cf09313e3c22557d24201ff0f156c22eb28a
│  │  ├─ f6
│  │  │  └─ ceedacffa235504d4e997d2e75e0d896868f61
│  │  ├─ f7
│  │  │  └─ 69b272632f923484c29a3131150a3a9b0c327f
│  │  ├─ f8
│  │  │  ├─ 99c40c68ca6da786172dc98292ec748ce0443c
│  │  │  └─ f6b02f286c55bce52d84f27eb012f4f11e2b0e
│  │  ├─ f9
│  │  │  ├─ 2afce6af14842edf8124c006325c5c4dd2b446
│  │  │  ├─ 83121477906ae2505f5cfb2a03e02c2f660716
│  │  │  └─ ad8e9ca04247f5c51a88578e96788a772be13e
│  │  ├─ fa
│  │  │  └─ d769cd167a94de6a293e8a7cc03556b2526c45
│  │  ├─ fc
│  │  │  ├─ 97a7405694db07c686e29c706afd894cf57912
│  │  │  └─ df9aee0169993ae1eda0b8680531c90917fa1c
│  │  ├─ fd
│  │  │  ├─ 349497aa8474c4e89ed8201894d2cf2eca5957
│  │  │  └─ be4637a650f7fc24daf37c2f7199f22e5ab3a8
│  │  ├─ ff
│  │  │  ├─ 50bc1350fc2be6f1f26dae79c361c37ec133dc
│  │  │  ├─ 50edac24e4f6af4d70161bd00729600ea59aa5
│  │  │  └─ a08852f9944bac842eca78bf4fcbbdfab2fdf6
│  │  ├─ info
│  │  └─ pack
│  │     ├─ pack-107d6b412e2489c26de07004f5cb52b17e418dbf.idx
│  │     └─ pack-107d6b412e2489c26de07004f5cb52b17e418dbf.pack
│  ├─ packed-refs
│  └─ refs
│     ├─ heads
│     │  ├─ main
│     │  └─ pratiksha-dev
│     ├─ remotes
│     │  └─ origin
│     │     ├─ HEAD
│     │     └─ pratiksha-dev
│     └─ tags
├─ .gitignore
├─ README.md
├─ analysis
│  └─ .gitkeep
├─ data
│  ├─ .gitkeep
│  ├─ dim_gender_sites.csv
│  └─ dim_location_administrative_units.csv
├─ data_tests
│  └─ .gitkeep
├─ dbt_project.yml
├─ macros
│  ├─ .gitkeep
│  ├─ commcare_default_case_properies.sql
│  ├─ extract_case_table_from_gender_commcare_json.sql
│  ├─ extract_case_table_from_wash_commcare_json.sql
│  ├─ generate_schema_name.sql
│  └─ validate_date.sql
├─ models
│  ├─ .DS_Store
│  ├─ marts
│  │  ├─ education
│  │  │  ├─ education_scholarship_categories_agg.sql
│  │  │  ├─ followup_attendance.sql
│  │  │  ├─ nudges.sql
│  │  │  ├─ parent_satisfaction.sql
│  │  │  ├─ parents_attendance.sql
│  │  │  ├─ public_partnerships.sql
│  │  │  ├─ student_satisfaction.sql
│  │  │  ├─ students_attendance.sql
│  │  │  ├─ teacher_satisfaction.sql
│  │  │  └─ well_being_sessions.sql
│  │  ├─ gender
│  │  │  ├─ case_occurence.sql
│  │  │  ├─ case_occurence_pii.sql
│  │  │  ├─ champions.sql
│  │  │  ├─ counselling.sql
│  │  │  ├─ gbv_leader.sql
│  │  │  ├─ life_skills_training_participants.sql
│  │  │  ├─ life_skills_training_sessions.sql
│  │  │  ├─ mh_score_improvement_descriptive.sql
│  │  │  ├─ safe_house.sql
│  │  │  ├─ safe_house_agg.sql
│  │  │  ├─ sessions_attended.sql
│  │  │  ├─ supported.sql
│  │  │  └─ youth_beneficiaries_disagg.sql
│  │  ├─ org_wide
│  │  │  ├─ county_footprint.sql
│  │  │  └─ mapping.sql
│  │  └─ wash
│  │     ├─ health_indicators.sql
│  │     ├─ health_indicators_schools_improved.sql
│  │     ├─ user_meters_mobiwater.sql
│  │     ├─ wash_facilities.sql
│  │     ├─ wash_marts_tests.yml
│  │     ├─ water_consumption_mobiwater.sql
│  │     └─ water_production.sql
│  ├─ source.yml
│  ├─ staging
│  │  ├─ education
│  │  │  ├─ staging_followup_attendance.sql
│  │  │  ├─ staging_nudges.sql
│  │  │  ├─ staging_parents_attendance.sql
│  │  │  ├─ staging_parents_satisfaction_survey.sql
│  │  │  ├─ staging_public_partnerships.sql
│  │  │  ├─ staging_scholarships.sql
│  │  │  ├─ staging_student_satisfaction_survey.sql
│  │  │  ├─ staging_students_attendance.sql
│  │  │  ├─ staging_teacher_satisfaction_survey.sql
│  │  │  └─ staging_well_being_sessions.sql
│  │  ├─ gender
│  │  │  ├─ staging_champions.sql
│  │  │  ├─ staging_gbv_leaders.sql
│  │  │  ├─ staging_gender_case_occurrences_commcare.sql
│  │  │  ├─ staging_gender_counselling_commcare.sql
│  │  │  ├─ staging_gender_final_mental_health_assesment.sql
│  │  │  ├─ staging_gender_initial_mental_health_assesment.sql
│  │  │  ├─ staging_gender_safe_house_commcare.sql
│  │  │  ├─ staging_gender_survivors_commcare.sql
│  │  │  ├─ staging_life_skills_training_participant_details.sql
│  │  │  ├─ staging_life_skills_training_session_details.sql
│  │  │  ├─ staging_youth_beneficiaries.sql
│  │  │  └─ survivors_data.sql
│  │  ├─ org_wide
│  │  │  ├─ staging_county_footprint.sql
│  │  │  └─ staging_mapping.sql
│  │  └─ wash
│  │     ├─ staging_facilities.sql
│  │     ├─ staging_health_indicators.sql
│  │     ├─ staging_meter_readings_mobiwater.sql
│  │     ├─ staging_user_meters_mobiwater.sql
│  │     ├─ staging_water_production.sql
│  │     └─ wash_staging_tests.yml
│  └─ wash_unit_tests.yml
├─ packages.yml
├─ seeds
│  ├─ dim_gender_sites.csv
│  └─ dim_location_administrative_units.csv
└─ snapshots
   └─ .gitkeep

```