import { motion } from "motion/react"

export function FadeIn(props: React.ComponentProps<typeof motion.div>) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      whileInView={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, ease: "easeOut" }}
      viewport={{ once: true }}
      {...props}
    />
  )
}
