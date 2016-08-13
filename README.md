mod_elasticsearch
=================

Add [tsloughter/erlastic_search](https://github.com/tsloughter/erlastic_search) and its
dependencies at their proper version (for Erlang 18) to your Zotonic deps in `zotonic.config`:

```erlang

  {deps,
    [
      %% ...
      {erlastic_search, ".*", {git, "https://github.com/tsloughter/erlastic_search.git", {tag, "master"}}},
      {hackney, ".*", {git, "https://github.com/benoitc/hackney.git", {tag, "1.6.1"}}},
      {jsx, ".*", {git, "https://github.com/talentdeficit/jsx.git", {tag, "2.8.0"}}}      
    ]
  }
```
