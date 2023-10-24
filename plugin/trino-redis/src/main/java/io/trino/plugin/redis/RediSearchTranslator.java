/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.redis;

import com.redis.lettucemod.search.AggregateOptions;
import com.redis.lettucemod.search.CursorOptions;
import com.redis.lettucemod.search.Limit;
import com.redis.lettucemod.search.SearchOptions;
import com.redis.lettucemod.search.SearchOptions.Builder;

import static java.util.Objects.requireNonNull;

public class RediSearchTranslator
{
    private final RediSearchQueryBuilder queryBuilder = new RediSearchQueryBuilder();

    private final RediSearchConfig config;

    public RediSearchTranslator(RediSearchConfig config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    public RediSearchConfig getConfig()
    {
        return config;
    }

    public static class Aggregation
    {
        private String index;
        private String query;
        private AggregateOptions<String, String> options;
        private CursorOptions cursorOptions;

        private Aggregation(Builder builder)
        {
            this.index = builder.index;
            this.query = builder.query;
            this.options = builder.options;
            this.cursorOptions = builder.cursorOptions;
        }

        public String getIndex()
        {
            return index;
        }

        public void setIndex(String index)
        {
            this.index = index;
        }

        public String getQuery()
        {
            return query;
        }

        public void setQuery(String query)
        {
            this.query = query;
        }

        public AggregateOptions<String, String> getOptions()
        {
            return options;
        }

        public CursorOptions getCursorOptions()
        {
            return cursorOptions;
        }

        public void setOptions(AggregateOptions<String, String> options)
        {
            this.options = options;
        }

        @Override
        public String toString()
        {
            return "Aggregation [index=" + index + ", query=" + query + ", options=" + options + ", cursorOptions="
                    + cursorOptions + "]";
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public static final class Builder
        {
            private String index;
            private String query;
            private AggregateOptions<String, String> options;
            private CursorOptions cursorOptions;

            private Builder()
            {
            }

            public Builder index(String index)
            {
                this.index = index;
                return this;
            }

            public Builder query(String query)
            {
                this.query = query;
                return this;
            }

            public Builder options(AggregateOptions<String, String> options)
            {
                this.options = options;
                return this;
            }

            public Builder cursorOptions(CursorOptions cursorOptions)
            {
                this.cursorOptions = cursorOptions;
                return this;
            }

            public Aggregation build()
            {
                return new Aggregation(this);
            }
        }
    }

    public static class Search
    {
        private String index;
        private String query;
        private SearchOptions<String, String> options;

        private Search(Builder builder)
        {
            this.index = builder.index;
            this.query = builder.query;
            this.options = builder.options;
        }

        public String getIndex()
        {
            return index;
        }

        public void setIndex(String index)
        {
            this.index = index;
        }

        public String getQuery()
        {
            return query;
        }

        public void setQuery(String query)
        {
            this.query = query;
        }

        public SearchOptions<String, String> getOptions()
        {
            return options;
        }

        public void setOptions(SearchOptions<String, String> options)
        {
            this.options = options;
        }

        @Override
        public String toString()
        {
            return "Search [index=" + index + ", query=" + query + ", options=" + options + "]";
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public static final class Builder
        {
            private String index;
            private String query;
            private SearchOptions<String, String> options;

            private Builder()
            {
            }

            public Builder index(String index)
            {
                this.index = index;
                return this;
            }

            public Builder query(String query)
            {
                this.query = query;
                return this;
            }

            public Builder options(SearchOptions<String, String> options)
            {
                this.options = options;
                return this;
            }

            public Search build()
            {
                return new Search(this);
            }
        }
    }

    public Search search(RediSearchTableHandle table, String[] columnNames)
    {
        String index = table.getIndex();
        String query = queryBuilder.buildQuery(table.getConstraint(), table.getWildcards());
        Builder<String, String> options = SearchOptions.builder();
        options.withScores(true);
        options.limit(Limit.offset(0).num(limit(table)));
        options.returnFields(columnNames);
        return Search.builder().index(index).query(query).options(options.build()).build();
    }

    public Aggregation aggregate(RediSearchTableHandle table, String[] columnNames)
    {
        String index = table.getIndex();
        String query = queryBuilder.buildQuery(table.getConstraint(), table.getWildcards());
        AggregateOptions.Builder<String, String> builder = AggregateOptions.builder();
        builder.load(RediSearchBuiltinField.KEY.getName());
        builder.loads(columnNames);
        queryBuilder.group(table).ifPresent(builder::operation);
        builder.operation(Limit.offset(0).num(limit(table)));
        AggregateOptions<String, String> options = builder.build();
        CursorOptions.Builder cursorOptions = CursorOptions.builder();
        if (config.getCursorCount() > 0) {
            cursorOptions.count(config.getCursorCount());
        }
        return Aggregation.builder().index(index).query(query).options(options).cursorOptions(cursorOptions.build())
                .build();
    }

    private long limit(RediSearchTableHandle tableHandle)
    {
        if (tableHandle.getLimit().isPresent()) {
            return tableHandle.getLimit().getAsLong();
        }
        return config.getDefaultLimit();
    }
}
