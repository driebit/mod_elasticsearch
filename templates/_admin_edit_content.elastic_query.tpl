{% extends "admin_edit_widget_std.tpl" %}

{# To edit the stored elastic search query #}

{% block widget_title %}
{_ Search query _}
<div class="widget-header-tools"></div>
{% endblock %}

{% block widget_show_minimized %}false{% endblock %}

{% block widget_content %}
{% with m.rsc[id] as r %}
<fieldset>
	<p class="notification notice">
		{_ Here you can edit your Elasticsearch query. _}
	</p>

    <div class="form-group">
    	<label class="control-label" for="elastic_query">{_ Elasticsearch query _}</label>
    	<div>
    	    {% with "[]" as placeholder %}
    	       <textarea class="form-control form-control--elasticquery" id="{{ #elastic_query }}" name="elastic_query" rows="15" placeholder="{{ placeholder }}">{{ r.elastic_query }}</textarea>
    	    {% endwith %}

            {% wire id=#elastic_query type="change" postback={elastic_query_preview query_type="query" rsc_id=id div_id=#elastic_query_preview target_id=#elastic_query index=m.config.mod_elasticsearch.index.value} delegate="controller_admin_elasticsearch_edit" %}
    	</div>
    </div>

    <div class="form-group">
        <a id="{{ #test_elastic_query }}" class="btn btn-default">{_ Test query _}</a>
        {% wire id=#test_elastic_query type="click" action={script script="$('#elastic_query').trigger('change')"} %}
    </div>

	<h4>{_ Query preview _}</h4>

	<div class="elastic-query-results" id="{{ #elastic_query_preview }}">
		{% catinclude "_admin_query_preview.tpl" id result=m.search[{query query_id=id id=id index=m.config.mod_elasticsearch.index.value pagelen=20}] %}
    </div>
</fieldset>
{% endwith %}
{% endblock %}
