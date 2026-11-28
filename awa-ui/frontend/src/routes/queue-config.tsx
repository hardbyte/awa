import { useQueueHub } from "@/hooks/use-queue-hub";
import { Time } from "@/components/Time";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import {
  DescriptionDetails,
  DescriptionList,
  DescriptionTerm,
} from "@/components/ui/description-list";
import {
  dlqPolicyIntent,
  dlqPolicyLabel,
  queueCapacityLabel,
  rateLimitLabel,
} from "@/components/RuntimeDisplay";

/** A queue's descriptor and the runtime configuration its workers report. */
export function QueueConfigTab() {
  const { queue, runtime } = useQueueHub();
  if (!queue) return null;
  const descriptorSeen = queue.descriptor_last_seen_at;

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader title="Descriptor" />
        <CardContent>
          <DescriptionList>
            <DescriptionTerm>Queue key</DescriptionTerm>
            <DescriptionDetails>
              <code className="rounded bg-muted px-1.5 py-0.5 text-sm">
                {queue.queue}
              </code>
            </DescriptionDetails>

            {queue.owner && (
              <>
                <DescriptionTerm>Owner</DescriptionTerm>
                <DescriptionDetails>{queue.owner}</DescriptionDetails>
              </>
            )}

            <DescriptionTerm>Descriptor health</DescriptionTerm>
            <DescriptionDetails>
              {queue.descriptor_mismatch ? (
                "Live runtimes disagree on this queue descriptor"
              ) : descriptorSeen ? (
                <span>
                  {queue.descriptor_stale ? "Stale" : "Live"} · seen <Time value={descriptorSeen} />
                </span>
              ) : (
                "Not declared by a live worker"
              )}
            </DescriptionDetails>

            {queue.docs_url && (
              <>
                <DescriptionTerm>Docs</DescriptionTerm>
                <DescriptionDetails>
                  <a
                    href={queue.docs_url}
                    target="_blank"
                    rel="noreferrer"
                    className="text-primary no-underline hover:underline"
                  >
                    {queue.docs_url}
                  </a>
                </DescriptionDetails>
              </>
            )}

            {queue.tags.length > 0 && (
              <>
                <DescriptionTerm>Tags</DescriptionTerm>
                <DescriptionDetails>
                  <div className="flex flex-wrap gap-1">
                    {queue.tags.map((tag) => (
                      <Badge key={tag} intent="secondary">
                        {tag}
                      </Badge>
                    ))}
                  </div>
                </DescriptionDetails>
              </>
            )}
          </DescriptionList>
        </CardContent>
      </Card>

      {runtime && (
        <Card>
          <CardHeader title="Runtime" />
          <CardContent>
            <DescriptionList>
              <DescriptionTerm>Instances</DescriptionTerm>
              <DescriptionDetails>
                {runtime.live_instances}/{runtime.instance_count} live
              </DescriptionDetails>

              <DescriptionTerm>Healthy instances</DescriptionTerm>
              <DescriptionDetails>{runtime.healthy_instances}</DescriptionDetails>

              <DescriptionTerm>In flight</DescriptionTerm>
              <DescriptionDetails>{runtime.total_in_flight}</DescriptionDetails>

              {runtime.config && (
                <>
                  <DescriptionTerm>Mode</DescriptionTerm>
                  <DescriptionDetails>
                    {runtime.config.mode === "weighted" ? "Weighted" : "Reserved"}
                  </DescriptionDetails>

                  <DescriptionTerm>Capacity</DescriptionTerm>
                  <DescriptionDetails>{queueCapacityLabel(runtime.config)}</DescriptionDetails>

                  <DescriptionTerm>Rate limit</DescriptionTerm>
                  <DescriptionDetails>{rateLimitLabel(runtime.config)}</DescriptionDetails>

                  <DescriptionTerm>Poll interval</DescriptionTerm>
                  <DescriptionDetails>{runtime.config.poll_interval_ms} ms</DescriptionDetails>

                  <DescriptionTerm>Deadline duration</DescriptionTerm>
                  <DescriptionDetails>{runtime.config.deadline_duration_secs} s</DescriptionDetails>

                  <DescriptionTerm>Priority aging</DescriptionTerm>
                  <DescriptionDetails>
                    {runtime.config.priority_aging_interval_secs} s
                  </DescriptionDetails>

                  <DescriptionTerm>DLQ policy</DescriptionTerm>
                  <DescriptionDetails>
                    <Badge intent={dlqPolicyIntent(runtime.config)}>
                      {dlqPolicyLabel(runtime.config)}
                    </Badge>
                  </DescriptionDetails>
                </>
              )}

              {runtime.config_mismatch && (
                <>
                  <DescriptionTerm>Config</DescriptionTerm>
                  <DescriptionDetails>
                    <Badge intent="warning">Mismatch</Badge>
                  </DescriptionDetails>
                </>
              )}
            </DescriptionList>
          </CardContent>
        </Card>
      )}
      {!runtime && (
        <p className="text-sm text-muted-fg">No live worker reports a configuration for this queue.</p>
      )}
    </div>
  );
}
