<div class="form-group">
    <div>
        {% button class="btn btn-default" text=_"Delete Elastic Search page index"
            action={confirm text=[
                                _"Are you sure you want to delete the Elastic Search index?",
                                "<br><br>",
                                _"You will need to rebuild the search index to fill it again."
                            ]
                            ok=_"Delete"
                            is_danger
                            postback={delete_index}
                            delegate=`mod_elasticsearch`}
        %}
        <span class="help-block">{_ Sometimes the Elastic Search index can get out of sync with the database. This will delete and recreate an empty Elastic Search index. _} {_ Use <b>Rebuild search indices</b> to fill the newly created Elastic Search index. _}</span>
    </div>
</div>
