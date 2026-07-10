import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Button,
  Card,
  Checkbox,
  Classes,
  H2,
  H3,
  H5,
  MenuItem,
  NumericInput,
  Spinner,
  Tab,
  Tabs,
  Tag
} from "@blueprintjs/core";
import { ItemRenderer, MultiSelect } from "@blueprintjs/select";

const API_BASE = import.meta.env.VITE_API_BASE ?? "";

type Story = {
  scope: string;
  story_id: string;
  story_title: string;
  synthesis: string;
  framing_notes: string;
  member_report_ids: string[];
  generated_at: string | null;
};

type Entity = {
  entity_id: string;
  entity_name: string;
  entity_type: "person" | "country" | "institution" | "organisation";
  total_mention_count: number;
  recent_mention_count: number;
  mention_velocity: number;
  salience_tier: string;
};

type Source = {
  source_id: string;
  name: string;
  active: number;
  last_status: string | null;
};

type Article = {
  report_id: string;
  title: string;
  url: string;
  source_domain: string | null;
  source_political_leaning: string | null;
  published_date: string | null;
  summary: string | null;
  topic: string | null;
  subtopics: string[];
  distilled_topics: string[];
  leaning: string | null;
  status: string;
};

type FilterOption = {
  value: string;
  label: string;
  count: number;
};

type FilterOptions = {
  categories: FilterOption[];
  topics: FilterOption[];
};

type StoryScopeStatus = {
  scope: string;
  status: "refreshed" | "empty" | "retained_error" | "retained_invalid";
  eligible_articles: number;
  story_count: number;
  detail: string | null;
};

type MonitoringStatus = {
  window_hours: number;
  reports: number;
  enriched: number;
  pending: number;
  failed: number;
  coverage_percent: number;
  last_polled_at: string | null;
  story_scopes: StoryScopeStatus[];
};

type UpdateResult = {
  llm_available: boolean;
  inserted: number;
  articles_before: number;
  articles_after: number;
  enriched_before: number;
  enriched_after: number;
  enrichment: {
    attempted: number;
    enriched: number;
    failed: number;
  };
  pending: number;
  failed: number;
  stories_refreshed: boolean;
  stories_skipped_reason: string | null;
  story_scopes: StoryScopeStatus[];
  stories_count: number;
  coverage: MonitoringStatus;
};

async function apiGet<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`);
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json();
}

async function apiPost<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json();
}

function useMercuryEvents(onMercuryEvent: () => void) {
  useEffect(() => {
    const source = new EventSource(`${API_BASE}/api/events`);
    const refreshEvents = ["article.new", "article.enriched", "stories.updated", "update.complete"];
    refreshEvents.forEach((eventName) => source.addEventListener(eventName, onMercuryEvent));
    return () => source.close();
  }, [onMercuryEvent]);
}

const OptionMultiSelect = MultiSelect.ofType<FilterOption>();

function FilterSelect({
  items,
  selected,
  placeholder,
  onChange
}: {
  items: FilterOption[];
  selected: string[];
  placeholder: string;
  onChange: (next: string[]) => void;
}) {
  const selectedItems = items.filter((item) => selected.includes(item.value));
  const renderItem: ItemRenderer<FilterOption> = (item, { handleClick, modifiers }) => {
    if (!modifiers.matchesPredicate) {
      return null;
    }
    const isSelected = selected.includes(item.value);
    return (
      <MenuItem
        active={modifiers.active}
        icon={isSelected ? "tick" : "blank"}
        key={item.value}
        label={`${item.count}`}
        onClick={handleClick}
        text={item.label}
      />
    );
  };

  return (
    <OptionMultiSelect
      fill
      itemPredicate={(query, item) => `${item.label} ${item.value}`.toLowerCase().includes(query.toLowerCase())}
      itemRenderer={renderItem}
      items={items}
      noResults={<MenuItem disabled text="No matches" />}
      onItemSelect={(item) =>
        onChange(
          selected.includes(item.value)
            ? selected.filter((value) => value !== item.value)
            : [...selected, item.value]
        )
      }
      placeholder={placeholder}
      selectedItems={selectedItems}
      tagRenderer={(item) => item.label}
      tagInputProps={{
        onRemove: (_tag, index) => {
          const removed = selectedItems[index];
          if (removed) {
            onChange(selected.filter((value) => value !== removed.value));
          }
        }
      }}
    />
  );
}

function StoryCards({ stories, emptyMessage = "No story clusters yet" }: { stories: Story[]; emptyMessage?: string }) {
  if (!stories.length) {
    return <div className="empty-state">{emptyMessage}</div>;
  }
  return (
    <div className="story-stack">
      {stories.map((story) => (
        <Card className="story-card" key={`${story.scope}-${story.story_id}`}>
          <div className="story-card__head">
            <H5>{story.story_title}</H5>
            <Tag minimal>{story.member_report_ids.length} reports</Tag>
          </div>
          <p>{story.synthesis}</p>
          <p className="muted">{story.framing_notes}</p>
        </Card>
      ))}
    </div>
  );
}

function TrendingChart({ entities }: { entities: Entity[] }) {
  const [selectedType, setSelectedType] = useState<string>("person");
  const grouped = useMemo(() => {
    return entities.reduce<Record<string, Entity[]>>((acc, entity) => {
      const key = entity.entity_type || "other";
      acc[key] = [...(acc[key] ?? []), entity];
      return acc;
    }, {});
  }, [entities]);
  const entityTypes = ["person", "organisation", "institution", "country"];

  if (!entities.length) {
    return <div className="empty-state">No entity mentions yet</div>;
  }

  function renderBars(type: string) {
    const values = grouped[type] ?? [];
    const max = Math.max(1, ...values.map((entity) => entity.recent_mention_count));

    if (!values.length) {
      return <div className="empty-state">No {type} mentions yet</div>;
    }

    return (
      <div className="entity-bars">
        {values.slice(0, 10).map((entity) => (
          <div className="entity-row" key={entity.entity_id}>
            <span className="entity-name">{entity.entity_name}</span>
            <div className="entity-track">
              <div className="entity-fill" style={{ width: `${(entity.recent_mention_count / max) * 100}%` }} />
            </div>
            <span className="entity-count">{entity.recent_mention_count}</span>
          </div>
        ))}
      </div>
    );
  }

  return (
    <Tabs id="entity-tracking" selectedTabId={selectedType} onChange={(tabId) => setSelectedType(String(tabId))}>
      {entityTypes.map((type) => (
        <Tab
          id={type}
          key={type}
          panel={renderBars(type)}
          title={type === "person" ? "Person" : type.charAt(0).toUpperCase() + type.slice(1)}
        />
      ))}
    </Tabs>
  );
}

function LatestReports({ articles }: { articles: Article[] }) {
  if (!articles.length) {
    return <div className="empty-state">No reports ingested yet</div>;
  }

  return (
    <div className="latest-list">
      {articles.map((article) => (
        <a className="latest-row" href={article.url} key={article.report_id} rel="noreferrer" target="_blank">
          <span className="latest-title">{article.title}</span>
          <span className="latest-meta">
            <span>{article.source_domain ?? "Unknown"}</span>
            {article.source_political_leaning && <Tag minimal>{article.source_political_leaning}</Tag>}
            <Tag minimal>{article.status}</Tag>
          </span>
        </a>
      ))}
    </div>
  );
}

export default function App() {
  const [domesticStories, setDomesticStories] = useState<Story[]>([]);
  const [polandUkStories, setPolandUkStories] = useState<Story[]>([]);
  const [focusedStories, setFocusedStories] = useState<Story[]>([]);
  const [entities, setEntities] = useState<Entity[]>([]);
  const [sources, setSources] = useState<Source[]>([]);
  const [latestArticles, setLatestArticles] = useState<Article[]>([]);
  const [filterOptions, setFilterOptions] = useState<FilterOptions>({ categories: [], topics: [] });
  const [monitoring, setMonitoring] = useState<MonitoringStatus | null>(null);
  const [categories, setCategories] = useState<string[]>([]);
  const [topics, setTopics] = useState<string[]>([]);
  const [selectedTab, setSelectedTab] = useState<string>("domestic");
  const [isAnalysing, setIsAnalysing] = useState(false);
  const [isUpdating, setIsUpdating] = useState(false);
  const [updateLimit, setUpdateLimit] = useState(12);
  const [refreshStories, setRefreshStories] = useState(true);
  const [isUpdateOpen, setIsUpdateOpen] = useState(false);
  const [lastUpdate, setLastUpdate] = useState<UpdateResult | null>(null);
  const [isDark, setIsDark] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);

  const refreshFromEvents = useCallback(() => setRefreshKey((value) => value + 1), []);
  useMercuryEvents(refreshFromEvents);

  useEffect(() => {
    let cancelled = false;
    Promise.all([
      apiGet<Story[]>("/api/stories/domestic"),
      apiGet<Story[]>("/api/stories/poland_uk"),
      apiGet<Entity[]>("/api/entities/trending?limit=10&exclude=Poland"),
      apiGet<Source[]>("/api/sources"),
      apiGet<Article[]>("/api/articles?limit=10"),
      apiGet<FilterOptions>("/api/filters"),
      apiGet<MonitoringStatus>("/api/monitoring/status")
    ])
      .then(([domestic, polandUk, entityRows, sourceRows, articleRows, filters, status]) => {
        if (cancelled) {
          return;
        }
        setDomesticStories(domestic);
        setPolandUkStories(polandUk);
        setEntities(entityRows);
        setSources(sourceRows);
        setLatestArticles(articleRows);
        setFilterOptions(filters);
        setMonitoring(status);
        setError(null);
      })
      .catch((err: Error) => {
        if (!cancelled) {
          setError(err.message);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [refreshKey]);

  const activeSources = sources.filter((source) => source.active).length;

  async function runUpdate() {
    setIsUpdating(true);
    setError(null);
    try {
      const result = await apiPost<UpdateResult>("/api/update", {
        poll_feeds: true,
        enrich_limit: updateLimit,
        refresh_stories: refreshStories
      });
      setLastUpdate(result);
      setMonitoring(result.coverage);
      setRefreshKey((value) => value + 1);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Update failed");
    } finally {
      setIsUpdating(false);
    }
  }

  async function runAnalysis() {
    setIsAnalysing(true);
    setError(null);
    try {
      const stories = await apiPost<Story[]>("/api/analyse", { categories, topics });
      setFocusedStories(stories);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Analysis failed");
    } finally {
      setIsAnalysing(false);
    }
  }

  return (
    <main className={isDark ? Classes.DARK : ""}>
      <div className="app-shell">
        <header className="app-header">
          <div className="brand-row">
            <H2>Mercury</H2>
            <div className="header-status">
              <span>Media Monitoring</span>
              <div className="header-meta">
                <Tag intent={activeSources ? "success" : "warning"}>{activeSources} active feeds</Tag>
                <Tag minimal>{entities.length} tracked entities</Tag>
                {monitoring && (
                  <Tag intent={monitoring.pending > 0 ? "warning" : "success"} minimal>
                    {monitoring.enriched}/{monitoring.reports} enriched · {monitoring.window_hours}h
                  </Tag>
                )}
              </div>
            </div>
          </div>
          <div className="header-actions">
            <Button
              icon="cloud-download"
              intent={isUpdateOpen ? "primary" : undefined}
              onClick={() => setIsUpdateOpen((value) => !value)}
            >
              Manual update
            </Button>
            <Button icon={isDark ? "flash" : "moon"} onClick={() => setIsDark((value) => !value)} />
          </div>
        </header>

        {error && <div className="error-band">{error}</div>}

        {isUpdateOpen && (
          <section className="panel update-panel">
            <div>
              <H3>Manual Update</H3>
              <div className="snapshot-meta">
                Rolling {monitoring?.window_hours ?? 24}h snapshot
                {monitoring?.last_polled_at ? ` · last poll ${monitoring.last_polled_at} UTC` : " · not polled yet"}
              </div>
              <div className="update-stats">
                <Tag minimal>{lastUpdate ? `${lastUpdate.inserted} new reports` : "New reports only"}</Tag>
                <Tag minimal>{monitoring ? `${monitoring.enriched}/${monitoring.reports} enriched` : "Coverage loading"}</Tag>
                <Tag intent={monitoring?.pending ? "warning" : "success"} minimal>
                  {monitoring ? `${monitoring.pending} pending` : "Pending loading"}
                </Tag>
                <Tag minimal>
                  {lastUpdate
                    ? lastUpdate.stories_refreshed
                      ? "stories refreshed"
                      : lastUpdate.stories_skipped_reason ?? "stories unchanged"
                    : `${activeSources} active feeds`}
                </Tag>
              </div>
            </div>
            <div className="update-controls">
              <label className="number-control">
                <span>Enrich</span>
                <NumericInput
                  allowNumericCharactersOnly
                  buttonPosition="right"
                  clampValueOnBlur
                  fill
                  max={100}
                  min={0}
                  onValueChange={(value) => setUpdateLimit(Number.isFinite(value) ? value : 0)}
                  value={updateLimit}
                />
              </label>
              <Checkbox
                checked={refreshStories}
                label="Refresh stories"
                onChange={() => setRefreshStories((value) => !value)}
              />
              <Button icon="cloud-download" intent="primary" loading={isUpdating} onClick={runUpdate}>
                Update now
              </Button>
            </div>
          </section>
        )}

        <section className="dashboard-grid">
          <div className="panel panel-stories">
            <Tabs id="top-stories" selectedTabId={selectedTab} onChange={(tabId) => setSelectedTab(String(tabId))}>
              <Tab
                id="domestic"
                title="Top Domestic Stories"
                panel={
                  <StoryCards
                    emptyMessage={`No multi-source stories detected in the last ${monitoring?.window_hours ?? 24} hours`}
                    stories={domesticStories}
                  />
                }
              />
              <Tab
                id="poland_uk"
                title="Top Poland-UK Stories"
                panel={
                  <StoryCards
                    emptyMessage={`No multi-source stories detected in the last ${monitoring?.window_hours ?? 24} hours`}
                    stories={polandUkStories}
                  />
                }
              />
            </Tabs>
          </div>

          <div className="panel panel-trending">
            <H3>Trending</H3>
            <TrendingChart entities={entities} />
          </div>

          <aside className="panel panel-filters">
            <H3>Filters</H3>
            <label>
              <span>Category</span>
              <FilterSelect
                items={filterOptions.categories}
                selected={categories}
                placeholder="Select categories"
                onChange={setCategories}
              />
            </label>
            <label>
              <span>Topic</span>
              <FilterSelect
                items={filterOptions.topics}
                selected={topics}
                placeholder="Select topics"
                onChange={setTopics}
              />
            </label>
            <Button fill icon="search" intent="primary" loading={isAnalysing} onClick={runAnalysis}>
              Analyse
            </Button>
          </aside>

          <div className="panel panel-focused">
            <div className="focused-head">
              <H3>Focused Analysis</H3>
              {isAnalysing && <Spinner size={20} />}
            </div>
            <StoryCards stories={focusedStories} />
          </div>

          <div className="panel panel-latest">
            <H3>Latest Reports</H3>
            <LatestReports articles={latestArticles} />
          </div>
        </section>
      </div>
    </main>
  );
}
