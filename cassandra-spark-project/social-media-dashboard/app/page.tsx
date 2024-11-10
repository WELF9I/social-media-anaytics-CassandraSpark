"use client"

import React, { useState, useEffect } from 'react'
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { AlertCircle, ArrowUpRight, TrendingUp, Users, Activity } from 'lucide-react'

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import {
  Pagination,
  PaginationContent,
  PaginationEllipsis,
  PaginationItem,
  PaginationLink,
  PaginationNext,
  PaginationPrevious,
} from "@/components/ui/pagination"

interface PostStats {
  post_id: string
  likes: number
  shares: number
  comments: number
  hashtags: string[]
  display_id?: number
}

interface HashtagStats {
  hashtag: string
  usage_count: number
}

export default function AnalyticsDashboard() {
  const [postStats, setPostStats] = useState<PostStats[]>([])
  const [hashtagStats, setHashtagStats] = useState<HashtagStats[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [currentPage, setCurrentPage] = useState(1)
  const itemsPerPage = 5

  const fetchData = async () => {
    try {
      setLoading(true)
      const [postsRes, hashtagsRes] = await Promise.all([
        fetch('http://localhost:8000/api/posts/stats/'),
        fetch('http://localhost:8000/api/hashtags/stats/')
      ])
      
      const posts = await postsRes.json()
      const hashtags = await hashtagsRes.json()
      
      const transformedPosts = Array.isArray(posts) 
        ? posts.map((post, index) => ({
            ...post,
            display_id: index + 1
          }))
        : []
      
      setPostStats(transformedPosts)
      setHashtagStats(Array.isArray(hashtags) ? hashtags : [])
      setError(null)
    } catch (err) {
      setError('Error loading data')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchData()
    const interval = setInterval(fetchData, 1000)
    return () => clearInterval(interval)
  }, [])

  const totalLikes = postStats.reduce((sum, post) => sum + post.likes, 0)
  const totalShares = postStats.reduce((sum, post) => sum + post.shares, 0)
  const totalComments = postStats.reduce((sum, post) => sum + post.comments, 0)

  // Pagination calculations
  const totalPages = Math.ceil(postStats.length / itemsPerPage)
  const startIndex = (currentPage - 1) * itemsPerPage
  const endIndex = startIndex + itemsPerPage
  const currentPosts = postStats.slice(startIndex, endIndex)

  // Generate page numbers array
  const getPageNumbers = () => {
    const pageNumbers = []
    for (let i = 1; i <= totalPages; i++) {
      if (
        i === 1 ||
        i === totalPages ||
        (i >= currentPage - 1 && i <= currentPage + 1)
      ) {
        pageNumbers.push(i)
      } else if (i === currentPage - 2 || i === currentPage + 2) {
        pageNumbers.push('...')
      }
    }
    return pageNumbers
  }

  return (
    <div className="flex-col md:flex ">
      <div className="border-b">
        <div className="flex h-16 items-center px-4">
          <h1 className="text-3xl font-bold tracking-tight">Social Media Analytics</h1>
          <Badge variant="secondary" className="ml-auto">
            Real-time Updates
          </Badge>
        </div>
      </div>
      <div className="flex-1 space-y-4 p-8 pt-6">
        {error && (
          <Alert variant="destructive">
            <AlertCircle className="h-4 w-4" />
            <AlertTitle>Error</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        {/* Updated to use flex row with centered alignment, larger widths, and spacing */}
        <div className="flex justify-center w-full">
          <div className="flex flex-row gap-6 w-full max-w-5xl px-4"> {/* Centers cards and adds max width */}
            <Card className="flex-1 p-4">
              <CardHeader className="flex flex-row items-center justify-between pb-2">
                <CardTitle className="text-sm font-medium">Total Likes</CardTitle>
                <TrendingUp className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{totalLikes}</div>
              </CardContent>
            </Card>
            <Card className="flex-1 p-4">
              <CardHeader className="flex flex-row items-center justify-between pb-2">
                <CardTitle className="text-sm font-medium">Total Shares</CardTitle>
                <ArrowUpRight className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{totalShares}</div>
              </CardContent>
            </Card>
            <Card className="flex-1 p-4">
              <CardHeader className="flex flex-row items-center justify-between pb-2">
                <CardTitle className="text-sm font-medium">Total Comments</CardTitle>
                <Users className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{totalComments}</div>
              </CardContent>
            </Card>
            <Card className="flex-1 p-4">
              <CardHeader className="flex flex-row items-center justify-between pb-2">
                <CardTitle className="text-sm font-medium">Active Posts</CardTitle>
                <Activity className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{postStats.length}</div>
              </CardContent>
            </Card>
          </div>
        </div>


        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-7">
          <Card className="col-span-4">
            <CardHeader>
              <CardTitle>Engagement per Post</CardTitle>
            </CardHeader>
            <CardContent className="pl-2">
              <ResponsiveContainer width="100%" height={350}>
                <LineChart data={postStats}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="display_id" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Line type="monotone" dataKey="likes" stroke="#8884d8" />
                  <Line type="monotone" dataKey="shares" stroke="#82ca9d" />
                  <Line type="monotone" dataKey="comments" stroke="#ffc658" />
                </LineChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
          <Card className="col-span-3">
            <CardHeader>
              <CardTitle>Top Hashtags</CardTitle>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={350}>
                <BarChart data={hashtagStats.slice(0, 5)} layout="vertical">
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis type="number" />
                  <YAxis dataKey="hashtag" type="category" />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey="usage_count" fill="#8884d8" />
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Recent Posts</CardTitle>
          </CardHeader>
          <CardContent>
            <Tabs defaultValue="realtime" className="w-full">
              <TabsList>
                <TabsTrigger value="realtime">Real-time</TabsTrigger>
                <TabsTrigger value="historical">Historical</TabsTrigger>
              </TabsList>
              <TabsContent value="realtime">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-[100px]">Post ID</TableHead>
                      <TableHead>Likes</TableHead>
                      <TableHead>Shares</TableHead>
                      <TableHead>Comments</TableHead>
                      <TableHead className="w-[200px]">Hashtags</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {currentPosts.map((post) => (
                      <TableRow key={post.post_id}>
                        <TableCell className="font-medium">{post.display_id}</TableCell>
                        <TableCell>{post.likes}</TableCell>
                        <TableCell>{post.shares}</TableCell>
                        <TableCell>{post.comments}</TableCell>
                        <TableCell>{post.hashtags.slice(0, 3).join(', ')}{post.hashtags.length > 3 ? '...' : ''}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
                
                <div className="mt-4 flex justify-center">
                  <Pagination>
                    <PaginationContent>
                      <PaginationItem>
                        <PaginationPrevious 
                          onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
                          //@ts-ignore
                          disabled={currentPage === 1}
                        />
                      </PaginationItem>
                      
                      {getPageNumbers().map((pageNum, idx) => (
                        <PaginationItem key={idx}>
                          {pageNum === '...' ? (
                            <PaginationEllipsis />
                          ) : (
                            <PaginationLink
                              onClick={() => setCurrentPage(Number(pageNum))}
                              isActive={currentPage === pageNum}
                            >
                              {pageNum}
                            </PaginationLink>
                          )}
                        </PaginationItem>
                      ))}

                      <PaginationItem>
                        <PaginationNext 
                          onClick={() => setCurrentPage(prev => Math.min(totalPages, prev + 1))}
                          //@ts-ignore
                          disabled={currentPage === totalPages}
                        />
                      </PaginationItem>
                    </PaginationContent>
                  </Pagination>
                </div>
              </TabsContent>
              <TabsContent value="historical">
                <div className="flex h-[200px] items-center justify-center">
                  <p className="text-sm text-muted-foreground">Historical data view is not implemented yet.</p>
                </div>
              </TabsContent>
            </Tabs>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}