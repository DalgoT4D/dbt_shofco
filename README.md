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
│  │  ├─ 01
│  │  │  ├─ 866f73d178f6834e3b3957cfce4b276ebd0f06
│  │  │  └─ ad3dc94e56b48c1dad8a257a4c78d8087ccadc
│  │  ├─ 04
│  │  │  └─ 19203321d0b1e78a3c9eff576d8fcbcd5de0db
│  │  ├─ 08
│  │  │  └─ f73de2aa9cd58284d4d27dc140cff066cf709b
│  │  ├─ 0a
│  │  │  └─ 72014183ada440564da66f3406573ff1c06cb8
│  │  ├─ 0e
│  │  │  └─ 045f1fd8e89f23aeca7ad3141fd116277bb6a5
│  │  ├─ 0f
│  │  │  ├─ 1b77109246d4e2859546103740bb2d0e271970
│  │  │  └─ 5ace8a3ab3d46d710377c422613c67c59d2720
│  │  ├─ 10
│  │  │  └─ 1ed2f0b51c907c39d9d0d620429bbea17260a1
│  │  ├─ 12
│  │  │  └─ 906b6cabcb14ffc08282ba6410f47e5c8fa65d
│  │  ├─ 14
│  │  │  ├─ 3e57f8589d77322f72d90740c4256e08a3008c
│  │  │  ├─ d446c5bfb321a8afb6b858bb1ce04689cf3674
│  │  │  └─ f12192adbd06deb82ec02495c711abca10498b
│  │  ├─ 15
│  │  │  └─ 8ed962c9bc5112337acb6eba6d4ddcf267649f
│  │  ├─ 17
│  │  │  └─ 60b7258360a94589933440053740e842206098
│  │  ├─ 1b
│  │  │  └─ 1f232ad16116507d36bafcdb252e415c2c67d8
│  │  ├─ 1e
│  │  │  └─ 0b6c933210b59d7053a51adcf38281de64e1e4
│  │  ├─ 23
│  │  │  └─ 386a94cfc7ddc2ea183de47b2db34a6f4927a3
│  │  ├─ 24
│  │  │  └─ 3aa59a20a1792220c1a9229c5737b286b0ca8c
│  │  ├─ 2e
│  │  │  └─ fc878c18d9360a385cda9d131f5163d43b0b9c
│  │  ├─ 33
│  │  │  └─ 9853dbe114a4ddde2cc5c3503dffd4fc912a9c
│  │  ├─ 3d
│  │  │  └─ 11811fcd263a0dd684d1195143441d71dd0299
│  │  ├─ 3e
│  │  │  └─ 130a01dac47ab9f9adf63a729f60a1f15fa154
│  │  ├─ 3f
│  │  │  ├─ 50d48ef316f3a7712f5b3f0a9bcfe91ac05cf4
│  │  │  ├─ 8784373ff5c08fcf43db5eaf7debb843df1a57
│  │  │  └─ ef050becfd32689739a39f4e8d886fbd9b1086
│  │  ├─ 40
│  │  │  └─ a5991898bd84423f949cec7efc12646d8597db
│  │  ├─ 44
│  │  │  └─ 3ad974e2ef19a889ae03b762a2c2b132766a55
│  │  ├─ 46
│  │  │  └─ b93873382bcbbe6ce3b825851a92b983ca2a03
│  │  ├─ 4c
│  │  │  └─ 4548d29a82cd44ff1ae0bfc2c8f6c8cd384693
│  │  ├─ 4d
│  │  │  ├─ 3132c5039d86c1e7da3da6ae964562b2d6dbe4
│  │  │  ├─ 7771e4a2a6b54b358dacbaf8df9b5b9bc09142
│  │  │  └─ 91548f4cde9d2afbf1a6ace0ee0c7659ea70b3
│  │  ├─ 4f
│  │  │  ├─ 7d0a8f74dfde40d7cf42683dfe75746f59484e
│  │  │  └─ 8cf5b9b1acca10aab25f53a7e94db167afa228
│  │  ├─ 50
│  │  │  └─ 0bb66b242ff595ecf5a30066c62d652e51085a
│  │  ├─ 53
│  │  │  └─ c0155304733e6e5a312e616fec256257a0116f
│  │  ├─ 55
│  │  │  ├─ 0d9a6cea2c909e170dc15418b16c21f057b1bb
│  │  │  └─ dfb05cb72971d935b712c30c97c5484aba12a1
│  │  ├─ 59
│  │  │  └─ aa365ade53db0f696ab33e9dcbe2bfc7137aa7
│  │  ├─ 5b
│  │  │  └─ 324a3aacb137e66988cc034a8739e420137738
│  │  ├─ 5c
│  │  │  ├─ 504e42471efae4c601cab9fbc23004a6949ca8
│  │  │  └─ 522f79deec94d8a9e28fc66f97ed6eb0b26b37
│  │  ├─ 5e
│  │  │  ├─ 47c53b6ea6a075a446fe813ffe6f44c78d7690
│  │  │  └─ fd62dd34c59256e8aa4e7da13b7feab5808257
│  │  ├─ 5f
│  │  │  └─ 59bc7a96a279b9239ed635f7c486f8187d8c99
│  │  ├─ 63
│  │  │  └─ c56f69a5348e3312b14dd3dcbf2d39ff3867b8
│  │  ├─ 6b
│  │  │  └─ 50feb62c4950f1d4d6398b459e28088e52d464
│  │  ├─ 6d
│  │  │  └─ 25602f12e224bac75fa570d98a1462e3e57ee5
│  │  ├─ 70
│  │  │  └─ c017d8aa1ee6808c63e5233ab5b345b9974d34
│  │  ├─ 74
│  │  │  └─ 3079c89ba20904f8f56ca05bc314c6bb8a1306
│  │  ├─ 75
│  │  │  ├─ 2495b3d7ec56c810a14da23cf5e7cab80df32e
│  │  │  └─ 28f66bde2d266351697caf4fd23711532e15b1
│  │  ├─ 77
│  │  │  └─ 6743bb5b1121d5d28bd477e5ce2dce6c796845
│  │  ├─ 78
│  │  │  └─ 5757b848fc8295e8decd614d02efa70fd78931
│  │  ├─ 7c
│  │  │  └─ e9871602fcdc214e1df41aa184443153db759c
│  │  ├─ 81
│  │  │  ├─ 42cf24eb92185e78863a1d6fdf90c1f8101e1f
│  │  │  └─ 5eee38321ac32dd1ba55d0ad9587da668931e4
│  │  ├─ 82
│  │  │  └─ 9c4f749837b23847d8e0645101e0cc3643a69d
│  │  ├─ 84
│  │  │  └─ 5f721a9d6ae0934a8b05e3d3d7081cbf5646ec
│  │  ├─ 85
│  │  │  └─ 0b12375cea5ac8b841b28b2cefd88045f38abe
│  │  ├─ 89
│  │  │  └─ 9825c6ecabfe30b3e5b60e366026344da83c72
│  │  ├─ 95
│  │  │  └─ af1d4b583629dfbe7376365e6a40685e260b41
│  │  ├─ 96
│  │  │  └─ 4260a45838d0c7ac0b5b12420b5ddd11093eea
│  │  ├─ 99
│  │  │  └─ cc92ffe5b5d1db6cf35499b4c69f4dd7f94b78
│  │  ├─ 9b
│  │  │  ├─ 09e6284ae5f86a84374104496685491a9bbc9c
│  │  │  └─ 16fbb9bec8e4bccf1c58beccdbbd2a29fc53c6
│  │  ├─ 9e
│  │  │  └─ a2b30abf7d01ba628fc6fb4de397e2d3037162
│  │  ├─ 9f
│  │  │  └─ ad7aabdc40d79263a9e7483cdecf12385aa31c
│  │  ├─ a2
│  │  │  └─ e10bde3bc0520afc508ba37000f6b79fd83361
│  │  ├─ a5
│  │  │  └─ 3d5d8b421f58824cd4b95419ef6853b417e7be
│  │  ├─ a6
│  │  │  └─ 31760cc20daefb5442f6b75b2c788b8205cfc4
│  │  ├─ a8
│  │  │  └─ 0cecd9518354d0b8f10f6b43aa39321975d740
│  │  ├─ a9
│  │  │  └─ 875a9e9d88fca4d69185f72235c7edf3c0c6b4
│  │  ├─ ab
│  │  │  └─ fe356c1032b33851e87cd0d8309c722425f7c5
│  │  ├─ b7
│  │  │  └─ 672e264e2ec2d6fc322ee79647f73820ab0be9
│  │  ├─ b8
│  │  │  └─ 4c3d34186e8420a9ec2a441e5daa3ba51e57c8
│  │  ├─ b9
│  │  │  └─ 2ecda8138f2e3b0943a5ea00a8b2b580b8be6b
│  │  ├─ ba
│  │  │  └─ 5201e98b0c6f005109c4f24f9c629923cdd6a8
│  │  ├─ c0
│  │  │  ├─ e662aa11c4e0f89a2b75956f9e58d78773d1fb
│  │  │  └─ fa32a895e8b3b0a4e85ca4fc871cffeb79ba04
│  │  ├─ d0
│  │  │  └─ 3cb523d32f9d605886a347624c374056ee56ff
│  │  ├─ d5
│  │  │  ├─ 91e06f462e12a5535c676f46e2a1cedb13999e
│  │  │  └─ aa880b96e785c8c28447782f3dd543bcd11c9d
│  │  ├─ d7
│  │  │  └─ 072efb0d6b9e95bf5456f059433c3b47d31f93
│  │  ├─ da
│  │  │  └─ 74229138c99e70493efc4611bf74c49e673e19
│  │  ├─ dd
│  │  │  └─ ede036d0e218e39a7f07186b1c54260c139b51
│  │  ├─ e2
│  │  │  └─ e3b9e39e9a6a5273fc8cd24b844b7fb8ddd199
│  │  ├─ e4
│  │  │  └─ 89d32993d308b7daba268cc117ee357ec94965
│  │  ├─ e7
│  │  │  └─ 3df931fc863cf6772cff213e23efd20bef1a54
│  │  ├─ eb
│  │  │  └─ c5762210395053fecfdb8bdb2c71a3f41a23c2
│  │  ├─ ec
│  │  │  ├─ ea0eaa77e643e0f5949252888c1c624b486bae
│  │  │  └─ ef8d4945c62e906711c4b6486f8952f6423f72
│  │  ├─ ed
│  │  │  └─ b3923c2843f815c27b5133ee938ba140d381a1
│  │  ├─ f1
│  │  │  └─ 0a0ae13796d6923b24eff58f17f83f91505c02
│  │  ├─ f3
│  │  │  └─ 7517c4d5c057e4afd04ed4eecee8f4626b405f
│  │  ├─ f4
│  │  │  └─ 6379770073628611beb571eea42ea2021f9232
│  │  ├─ fc
│  │  │  └─ 97a7405694db07c686e29c706afd894cf57912
│  │  ├─ ff
│  │  │  └─ 50bc1350fc2be6f1f26dae79c361c37ec133dc
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
├─ dbt_project.yml
├─ macros
│  ├─ .gitkeep
│  ├─ commcare_default_case_properies.sql
│  ├─ extract_case_table_from_commcare_json.sql
│  ├─ extract_case_table_from_gender_commcare_json.sql
│  ├─ generate_schema_name.sql
│  └─ validate_date.sql
├─ models
│  ├─ .DS_Store
│  ├─ marts
│  │  ├─ education
│  │  │  └─ education_scholarship_categories_agg.sql
│  │  ├─ final_test.yml
│  │  ├─ gender
│  │  │  ├─ GBV_Leaders.sql
│  │  │  ├─ avg_satisfaction_scores.sql
│  │  │  ├─ case_occurence.sql
│  │  │  ├─ champions.sql
│  │  │  ├─ counselling.sql
│  │  │  ├─ duration_court_case.sql
│  │  │  ├─ mh_score_improvement_descriptive.sql
│  │  │  ├─ safe_house.sql
│  │  │  ├─ safe_house_agg.sql
│  │  │  ├─ sessions_attended.sql
│  │  │  ├─ supported.sql
│  │  │  └─ survivors_data.sql
│  │  ├─ gender_GBV_Leaders.sql
│  │  ├─ gender_avg_satisfaction_scores.sql
│  │  ├─ gender_case_occurence.sql
│  │  ├─ gender_champions.sql
│  │  ├─ gender_counselling.sql
│  │  ├─ gender_duration_court_case.sql
│  │  ├─ gender_mh_score_improvement_descriptive.sql
│  │  ├─ gender_safe_house.sql
│  │  ├─ gender_safe_house_agg.sql
│  │  ├─ gender_sessions_attended.sql
│  │  ├─ gender_supported.sql
│  │  └─ gender_survivors_data.sql
│  ├─ source.yml
│  └─ staging
│     ├─ education
│     │  ├─ staging_nudges.sql
│     │  ├─ staging_public_partnerships.sql
│     │  └─ staging_scholarships.sql
│     ├─ gender
│     │  ├─ staging_champions.sql
│     │  ├─ staging_gbv_leaders.sql
│     │  ├─ staging_gender_case_occurrences_commcare.sql
│     │  ├─ staging_gender_counselling_commcare.sql
│     │  ├─ staging_gender_final_mental_health_assesment.sql
│     │  ├─ staging_gender_initial_mental_health_assesment.sql
│     │  ├─ staging_gender_safe_house_commcare.sql
│     │  ├─ staging_gender_satisfaction_scores.sql
│     │  ├─ staging_gender_survivors_commcare.sql
│     │  ├─ staging_life_skills_training_participant_details.sql
│     │  ├─ staging_life_skills_training_session_details.sql
│     │  └─ staging_youth_beneficiaries.sql
│     ├─ staging_test.yml
│     ├─ stg_champions.sql
│     ├─ stg_gbv_leaders.sql
│     ├─ stg_gender_case_occurrences_commcare.sql
│     ├─ stg_gender_counselling_commcare.sql
│     ├─ stg_gender_final_mental_health_assesment.sql
│     ├─ stg_gender_initial_mental_health_assesment.sql
│     ├─ stg_gender_safe_house_commcare.sql
│     ├─ stg_gender_satisfaction_scores.sql
│     ├─ stg_gender_survivors_commcare.sql
│     ├─ stg_life_skills_training_participant_details.sql
│     ├─ stg_life_skills_training_session_details.sql
│     └─ stg_youth_beneficiaries.sql
├─ packages.yml
├─ snapshots
│  └─ .gitkeep
└─ tests
   └─ .gitkeep

```