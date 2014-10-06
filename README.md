mod_elasticsearch
=================

Add [tsloughter/erlastic_search](https://github.com/tsloughter/erlastic_search) to your
Zotonic deps in `zotonic.config`:

```erlang

  {deps,
    [
      %% ...
      { erlastic_search, "master", {git, "https://github.com/tsloughter/erlastic_search.git", {tag, "master"}}}
    ]
  }
```
